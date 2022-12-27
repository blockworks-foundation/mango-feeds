use anchor_client::{Cluster, solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair, account::Account}};
use anchor_lang::prelude::Pubkey;
use bytemuck::cast_slice;
use client::{Client, MangoGroupContext};
use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{pin_mut, SinkExt, StreamExt, future::{self, Ready}, TryStreamExt};
use log::*;
use std::{collections::{HashMap, HashSet}, fs::File, io::Read, net::SocketAddr, sync::Arc, sync::Mutex, time::Duration, convert::identity, str::FromStr};
use tokio::{
    net::{TcpListener, TcpStream},
    pin,
};
use tokio_tungstenite::tungstenite::{protocol::Message, Error};

use serde::Deserialize;
use solana_geyser_connector_lib::{metrics::{MetricType, MetricU64}, FilterConfig, fill_event_filter::SerumFillCheckpoint, StatusResponse};
use solana_geyser_connector_lib::{
    fill_event_filter::{self, FillCheckpoint, FillEventFilterMessage},
    grpc_plugin_source, metrics, websocket_source, MetricsConfig, SourceConfig,
};

type CheckpointMap = Arc<Mutex<HashMap<String, FillCheckpoint>>>;
type SerumCheckpointMap = Arc<Mutex<HashMap<String, SerumFillCheckpoint>>>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, Peer>>>;

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "command")]
pub enum Command {
    #[serde(rename = "subscribe")]
    Subscribe(SubscribeCommand),
    #[serde(rename = "unsubscribe")]
    Unsubscribe(UnsubscribeCommand),
    #[serde(rename = "getMarkets")]
    GetMarkets,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeCommand {
    pub market_id: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsubscribeCommand {
    pub market_id: String,
}

#[derive(Clone, Debug)]
pub struct Peer {
    pub sender: UnboundedSender<Message>,
    pub subscriptions: HashSet<String>,
}

async fn handle_connection_error(
    checkpoint_map: CheckpointMap,
    serum_checkpoint_map: SerumCheckpointMap,
    peer_map: PeerMap,
    market_ids: HashMap<String, String>,
    raw_stream: TcpStream,
    addr: SocketAddr,
    metrics_opened_connections: MetricU64,
    metrics_closed_connections: MetricU64,
) {
    metrics_opened_connections.clone().increment();

    let result = handle_connection(checkpoint_map, serum_checkpoint_map, peer_map.clone(), market_ids, raw_stream, addr).await;
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
    market_ids: HashMap<String, String>,
    raw_stream: TcpStream,
    addr: SocketAddr,
) -> Result<(), Error> {
    info!("ws connected: {}", addr);
    let ws_stream = tokio_tungstenite::accept_async(raw_stream).await?;
    let (ws_tx, ws_rx) = ws_stream.split();

    // 1: publish channel in peer map
    let (chan_tx, chan_rx) = unbounded();
    {
        peer_map.lock().unwrap().insert(
            addr,
            Peer {
                sender: chan_tx,
                subscriptions: HashSet::<String>::new(),
            },
        );
    }

    let receive_commands = ws_rx.try_for_each(|msg| {
        handle_commands(
            addr,
            msg,
            peer_map.clone(),
            checkpoint_map.clone(),
            serum_checkpoint_map.clone(),
            market_ids.clone(),
        )
    });
    let forward_updates = chan_rx.map(Ok).forward(ws_tx);

    pin_mut!(receive_commands, forward_updates);
    future::select(receive_commands, forward_updates).await;

    peer_map.lock().unwrap().remove(&addr);
    info!("ws disconnected: {}", &addr);
    Ok(())
}

fn handle_commands(
    addr: SocketAddr,
    msg: Message,
    peer_map: PeerMap,
    checkpoint_map: CheckpointMap,
    serum_checkpoint_map: SerumCheckpointMap,
    market_ids: HashMap<String, String>,
) -> Ready<Result<(), Error>> {
    let msg_str = msg.clone().into_text().unwrap();
    let command: Result<Command, serde_json::Error> = serde_json::from_str(&msg_str);
    let mut peers = peer_map.lock().unwrap();
    let peer = peers.get_mut(&addr).expect("peer should be in map");
    match command {
        Ok(Command::Subscribe(cmd)) => {
            let market_id = cmd.clone().market_id;
            match market_ids.get(&market_id) {
                None => {
                    let res = StatusResponse {
                        success: false,
                        message: "market not found",
                    };
                    peer.sender
                        .unbounded_send(Message::Text(serde_json::to_string(&res).unwrap()))
                        .unwrap();
                    return future::ok(());
                }
                _ => {}
            }
            let subscribed = peer.subscriptions.insert(market_id.clone());

            let res = if subscribed {
                StatusResponse {
                    success: true,
                    message: "subscribed",
                }
            } else {
                StatusResponse {
                    success: false,
                    message: "already subscribed",
                }
            };
            peer.sender
                .unbounded_send(Message::Text(serde_json::to_string(&res).unwrap()))
                .unwrap();

            if subscribed {
                // todo: this is janky af
                let checkpoint_map = checkpoint_map.lock().unwrap();
                let serum_checkpoint_map = serum_checkpoint_map.lock().unwrap();
                let checkpoint = checkpoint_map.get(&market_id);
                match checkpoint {
                    Some(checkpoint) => {
                        peer.sender
                            .unbounded_send(Message::Text(
                                serde_json::to_string(&checkpoint).unwrap(),
                            ))
                            .unwrap();
                    }
                    None => match serum_checkpoint_map.get(&market_id) {
                        Some(checkpoint) => {
                            peer.sender
                                .unbounded_send(Message::Text(
                                    serde_json::to_string(&checkpoint).unwrap(),
                                ))
                                .unwrap();
                        }
                        None => info!("no checkpoint available on client subscription"), // todo: what to do here?
                    }
                }
            }
        }
        Ok(Command::Unsubscribe(cmd)) => {
            info!("unsubscribe {}", cmd.market_id);
            let unsubscribed = peer.subscriptions.remove(&cmd.market_id);
            let res = if unsubscribed {
                StatusResponse {
                    success: true,
                    message: "unsubscribed",
                }
            } else {
                StatusResponse {
                    success: false,
                    message: "not subscribed",
                }
            };
            peer.sender
                .unbounded_send(Message::Text(serde_json::to_string(&res).unwrap()))
                .unwrap();
        }
        Ok(Command::GetMarkets) => {
            info!("getMarkets");
            peer.sender
                .unbounded_send(Message::Text(serde_json::to_string(&market_ids).unwrap()))
                .unwrap();
        }
        Err(err) => {
            info!("error deserializing user input {:?}", err);
            let res = StatusResponse {
                success: false,
                message: "invalid input",
            };
            peer.sender
                .unbounded_send(Message::Text(serde_json::to_string(&res).unwrap()))
                .unwrap();
        }
    };

    future::ok(())
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

    let a: Vec<(String, String)> = group_context
        .serum3_markets
        .iter()
        .map(|(_, context)| (context.market.name().to_owned(), context.market.serum_market_external.to_string())).collect();
    let b: Vec<(String, String)> = group_context
        .perp_markets
        .iter()
        .map(|(_, context)| (context.market.name().to_owned(), context.address.to_string())).collect();
    let market_pubkey_strings: HashMap<String, String> = [a, b]
        .concat()
        .into_iter()
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
                    for (addr, peer) in peer_copy.iter_mut() {
                        let json = serde_json::to_string(&update).unwrap();
                        
                        // only send updates if the peer is subscribed
                        if peer.subscriptions.contains(&update.market) {
                            let result = peer.sender.send(Message::Text(json)).await;
                            if result.is_err() {
                                error!(
                                    "ws update {} fill could not reach {}",
                                    update.market, addr
                                );
                            }
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
                    for (addr, peer) in peer_copy.iter_mut() {
                        let json = serde_json::to_string(&update).unwrap();
                        
                        // only send updates if the peer is subscribed
                        if peer.subscriptions.contains(&update.market) {
                            let result = peer.sender.send(Message::Text(json)).await;
                            if result.is_err() {
                                error!(
                                    "ws update {} fill could not reach {}",
                                    update.market, addr
                                );
                            }
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
                market_pubkey_strings.clone(),
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
