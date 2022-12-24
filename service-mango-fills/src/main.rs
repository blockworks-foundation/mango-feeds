use anchor_client::{Cluster, solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair, account::Account}};
use anchor_lang::prelude::Pubkey;
use bytemuck::cast_slice;
use client::{Client, MangoGroupContext};
use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{pin_mut, SinkExt, StreamExt};
use log::*;
use std::{collections::HashMap, fs::File, io::Read, net::SocketAddr, sync::Arc, sync::Mutex, time::Duration, convert::identity, str::FromStr};
use tokio::{
    net::{TcpListener, TcpStream},
    pin,
};
use tokio_tungstenite::tungstenite::{protocol::Message, Error};

use serde::Deserialize;
use solana_geyser_connector_lib::{metrics::{MetricType, MetricU64}, FilterConfig, fill_event_filter::SerumFillCheckpoint};
use solana_geyser_connector_lib::{
    fill_event_filter::{self, FillCheckpoint, FillEventFilterMessage},
    grpc_plugin_source, metrics, websocket_source, MetricsConfig, SourceConfig,
};

type CheckpointMap = Arc<Mutex<HashMap<String, FillCheckpoint>>>;
type SerumCheckpointMap = Arc<Mutex<HashMap<String, SerumFillCheckpoint>>>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, UnboundedSender<Message>>>>;

async fn handle_connection_error(
    checkpoint_map: CheckpointMap,
    serum_checkpoint_map: SerumCheckpointMap,
    peer_map: PeerMap,
    raw_stream: TcpStream,
    addr: SocketAddr,
    metrics_opened_connections: MetricU64,
    metrics_closed_connections: MetricU64,
) {
    metrics_opened_connections.clone().increment();

    let result = handle_connection(checkpoint_map, serum_checkpoint_map, peer_map.clone(), raw_stream, addr).await;
    if result.is_err() {
        error!("connection {} error {}", addr, result.unwrap_err());
    };

    metrics_closed_connections.clone().increment();

    peer_map.lock().unwrap().remove(&addr);
}

async fn handle_connection(
    checkpoint_map: CheckpointMap,
    serum_checkpoint_map: SerumCheckpointMap,
    peer_map: PeerMap,
    raw_stream: TcpStream,
    addr: SocketAddr,
) -> Result<(), Error> {
    info!("ws connected: {}", addr);
    let ws_stream = tokio_tungstenite::accept_async(raw_stream).await?;
    let (mut ws_tx, _ws_rx) = ws_stream.split();

    // 1: publish channel in peer map
    let (chan_tx, chan_rx) = unbounded();
    {
        peer_map.lock().unwrap().insert(addr, chan_tx);
        info!("ws published: {}", addr);
    }

    // todo: add subscribe logic
    // 2: send initial checkpoint
    {
        let checkpoint_map_copy = checkpoint_map.lock().unwrap().clone();
        for (_, ckpt) in checkpoint_map_copy.iter() {
            ws_tx
                .feed(Message::Text(serde_json::to_string(ckpt).unwrap()))
                .await?;
        }
    }
    info!("ws ckpt sent: {}", addr);
    ws_tx.flush().await?;

    // 3: forward all events from channel to peer socket
    let forward_updates = chan_rx.map(Ok).forward(ws_tx);
    pin_mut!(forward_updates);
    forward_updates.await?;

    info!("ws disconnected: {}", &addr);
    Ok(())
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub metrics: MetricsConfig,
    pub bind_ws_addr: String,
    pub rpc_http_url: String,
    pub mango_group: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Please enter a config file path argument.");

        return Ok(());
    }

    let config: Config = {
        let mut file = File::open(&args[1])?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        toml::from_str(&contents).unwrap()
    };

    solana_logger::setup_with_default("info");

    let metrics_tx = metrics::start(config.metrics, "fills".into());

    let metrics_opened_connections =
        metrics_tx.register_u64("fills_feed_opened_connections".into(), MetricType::Counter);

    let metrics_closed_connections =
        metrics_tx.register_u64("fills_feed_closed_connections".into(), MetricType::Counter);

    let rpc_url = config.rpc_http_url;
    let ws_url = rpc_url.replace("https", "wss");
    let rpc_timeout = Duration::from_secs(10);
    let cluster = Cluster::Custom(rpc_url.clone(), ws_url.clone());
    let client = Client::new(
        cluster.clone(),
        CommitmentConfig::processed(),
        &Keypair::new(),
        Some(rpc_timeout),
    );
    let group_context = Arc::new(MangoGroupContext::new_from_rpc(
        Pubkey::from_str(&config.mango_group).unwrap(),
        client.cluster.clone(),
        client.commitment,
    )?);

    let perp_queue_pks: Vec<(Pubkey, Pubkey)> = group_context
        .perp_markets
        .iter()
        .map(|(_, context)| (context.address, context.market.event_queue))
        .collect();

    let serum_market_pks: Vec<Pubkey> = group_context
        .serum3_markets
        .iter()
        .map(|(_, context)| context.market.serum_market_external)
        .collect();

    let serum_market_ais = client
        .rpc()
        .get_multiple_accounts(serum_market_pks.as_slice())?;
    let serum_market_ais: Vec<&Account> = serum_market_ais
        .iter()
        .filter_map(|maybe_ai| match maybe_ai {
            Some(ai) => Some(ai),
            None => None,
        })
        .collect();

    let serum_queue_pks: Vec<(Pubkey, Pubkey)> = serum_market_ais
        .iter()
        .enumerate()
        .map(|pair| {
            let market_state: serum_dex::state::MarketState = *bytemuck::from_bytes(
                &pair.1.data[5..5 + std::mem::size_of::<serum_dex::state::MarketState>()],
            );
            (serum_market_pks[pair.0], Pubkey::new(cast_slice(&identity(market_state.event_q) as &[_])))
        })
        .collect();

    let (account_write_queue_sender, slot_queue_sender, fill_receiver) =
    fill_event_filter::init(perp_queue_pks.clone(), serum_queue_pks.clone(), metrics_tx.clone()).await?;

    let checkpoints = CheckpointMap::new(Mutex::new(HashMap::new()));
    let serum_checkpoints = SerumCheckpointMap::new(Mutex::new(HashMap::new()));
    let peers = PeerMap::new(Mutex::new(HashMap::new()));

    let checkpoints_ref_thread = checkpoints.clone();
    let serum_checkpoints_ref_thread = serum_checkpoints.clone();
    let peers_ref_thread = peers.clone();

    // filleventfilter websocket sink
    tokio::spawn(async move {
        pin!(fill_receiver);
        loop {
            let message = fill_receiver.recv().await.unwrap();
            match message {
                FillEventFilterMessage::Update(update) => {
                    debug!("ws update {} {:?} fill", update.market, update.status);
                    let mut peer_copy = peers_ref_thread.lock().unwrap().clone();
                    for (k, v) in peer_copy.iter_mut() {
                        trace!("  > {}", k);
                        let json = serde_json::to_string(&update);
                        let result = v.send(Message::Text(json.unwrap())).await;
                        if result.is_err() {
                            error!(
                                "ws update {} {:?} fill could not reach {}",
                                update.market, update.status, k
                            );
                        }
                    }
                }
                FillEventFilterMessage::Checkpoint(checkpoint) => {
                    checkpoints_ref_thread
                        .lock()
                        .unwrap()
                        .insert(checkpoint.queue.clone(), checkpoint);
                }
                FillEventFilterMessage::SerumUpdate(update) => {
                    debug!("ws update {} {:?} serum fill", update.market, update.status);
                    let mut peer_copy = peers_ref_thread.lock().unwrap().clone();
                    for (k, v) in peer_copy.iter_mut() {
                        trace!("  > {}", k);
                        let json = serde_json::to_string(&update);
                        let result = v.send(Message::Text(json.unwrap())).await;
                        if result.is_err() {
                            error!(
                                "ws update {} {:?} serum fill could not reach {}",
                                update.market, update.status, k
                            );
                        }
                    }
                }
                FillEventFilterMessage::SerumCheckpoint(checkpoint) => {
                    serum_checkpoints_ref_thread
                        .lock()
                        .unwrap()
                        .insert(checkpoint.queue.clone(), checkpoint);
                }
            }
        }
    });

    info!("ws listen: {}", config.bind_ws_addr);
    let try_socket = TcpListener::bind(&config.bind_ws_addr).await;
    let listener = try_socket.expect("Failed to bind");
    tokio::spawn(async move {
        // Let's spawn the handling of each connection in a separate task.
        while let Ok((stream, addr)) = listener.accept().await {
            tokio::spawn(handle_connection_error(
                checkpoints.clone(),
                serum_checkpoints.clone(),
                peers.clone(),
                stream,
                addr,
                metrics_opened_connections.clone(),
                metrics_closed_connections.clone(),
            ));
        }
    });

    info!(
        "rpc connect: {}",
        config
            .source
            .grpc_sources
            .iter()
            .map(|c| c.connection_string.clone())
            .collect::<String>()
    );
    let use_geyser = true;
    let filter_config = FilterConfig {
        program_ids: vec![
            "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg".into(),
            "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX".into(),
        ],
    };
    if use_geyser {
        grpc_plugin_source::process_events(
            &config.source,
            &filter_config,
            account_write_queue_sender,
            slot_queue_sender,
            metrics_tx.clone(),
        )
        .await;
    } else {
        websocket_source::process_events(
            &config.source,
            account_write_queue_sender,
            slot_queue_sender,
        )
        .await;
    }

    Ok(())
}
