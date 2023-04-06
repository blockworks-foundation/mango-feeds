mod fill_event_filter;
mod fill_event_postgres_target;

use anchor_client::{
    solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair},
    Cluster,
};
use anchor_lang::prelude::Pubkey;
use client::{Client, MangoGroupContext};
use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{
    future::{self, Ready},
    pin_mut, SinkExt, StreamExt, TryStreamExt,
};
use log::*;
use mango_feeds_lib::{
    grpc_plugin_source, metrics,
    metrics::{MetricType, MetricU64},
    websocket_source, FilterConfig, MarketConfig, MetricsConfig, PostgresConfig, PostgresTlsConfig,
    SourceConfig, StatusResponse,
};
use service_mango_fills::{Command, FillCheckpoint, FillEventFilterMessage, FillEventType};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Read,
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    pin, time,
};
use tokio_tungstenite::tungstenite::{protocol::Message, Error};

use serde::Deserialize;

type CheckpointMap = Arc<Mutex<HashMap<String, FillCheckpoint>>>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, Peer>>>;

// jemalloc seems to be better at keeping the memory footprint reasonable over
// longer periods of time
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Clone, Debug)]
pub struct Peer {
    pub sender: UnboundedSender<Message>,
    pub market_subscriptions: HashSet<String>,
    pub account_subscriptions: HashSet<String>,
}

async fn handle_connection_error(
    checkpoint_map: CheckpointMap,
    peer_map: PeerMap,
    market_ids: HashMap<String, String>,
    raw_stream: TcpStream,
    addr: SocketAddr,
    metrics_opened_connections: MetricU64,
    metrics_closed_connections: MetricU64,
) {
    metrics_opened_connections.clone().increment();

    let result = handle_connection(
        checkpoint_map,
        peer_map.clone(),
        market_ids,
        raw_stream,
        addr,
    )
    .await;
    if result.is_err() {
        error!("connection {} error {}", addr, result.unwrap_err());
    };

    metrics_closed_connections.clone().increment();

    peer_map.lock().unwrap().remove(&addr);
}

async fn handle_connection(
    checkpoint_map: CheckpointMap,
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
                market_subscriptions: HashSet::<String>::new(),
                account_subscriptions: HashSet::<String>::new(),
            },
        );
    }

    let receive_commands = ws_rx.try_for_each(|msg| match msg {
        Message::Text(_) => handle_commands(
            addr,
            msg,
            peer_map.clone(),
            checkpoint_map.clone(),
            market_ids.clone(),
        ),
        Message::Ping(_) => {
            let peers = peer_map.clone();
            let mut peers_lock = peers.lock().unwrap();
            let peer = peers_lock.get_mut(&addr).expect("peer should be in map");
            peer.sender
                .unbounded_send(Message::Pong(Vec::new()))
                .unwrap();
            future::ready(Ok(()))
        }
        _ => future::ready(Ok(())),
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
    market_ids: HashMap<String, String>,
) -> Ready<Result<(), Error>> {
    let msg_str = msg.clone().into_text().unwrap();
    let command: Result<Command, serde_json::Error> = serde_json::from_str(&msg_str);
    let mut peers = peer_map.lock().unwrap();
    let peer = peers.get_mut(&addr).expect("peer should be in map");

    match command {
        Ok(Command::Subscribe(cmd)) => {
            let mut wildcard = true;
            // DEPRECATED
            match cmd.market_id {
                Some(market_id) => {
                    wildcard = false;
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
                    let subscribed = peer.market_subscriptions.insert(market_id.clone());

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
                        let checkpoint_map = checkpoint_map.lock().unwrap();
                        let checkpoint = checkpoint_map.get(&market_id);
                        match checkpoint {
                            Some(checkpoint) => {
                                peer.sender
                                    .unbounded_send(Message::Text(
                                        serde_json::to_string(&checkpoint).unwrap(),
                                    ))
                                    .unwrap();
                            }
                            None => info!(
                                "no checkpoint available on client subscription for market {}",
                                &market_id
                            ),
                        };
                    }
                }
                None => {}
            }
            match cmd.market_ids {
                Some(cmd_market_ids) => {
                    wildcard = false;
                    for market_id in cmd_market_ids {
                        match market_ids.get(&market_id) {
                            None => {
                                let res = StatusResponse {
                                    success: false,
                                    message: &format!("market {} not found", &market_id),
                                };
                                peer.sender
                                    .unbounded_send(Message::Text(
                                        serde_json::to_string(&res).unwrap(),
                                    ))
                                    .unwrap();
                                return future::ok(());
                            }
                            _ => {}
                        }
                        if peer.market_subscriptions.insert(market_id.clone()) {
                            let checkpoint_map = checkpoint_map.lock().unwrap();
                            let checkpoint = checkpoint_map.get(&market_id);
                            let res = StatusResponse {
                                success: true,
                                message: &format!("subscribed to market {}", &market_id),
                            };

                            peer.sender
                                .unbounded_send(Message::Text(serde_json::to_string(&res).unwrap()))
                                .unwrap();
                            match checkpoint {
                                Some(checkpoint) => {
                                    peer.sender
                                        .unbounded_send(Message::Text(
                                            serde_json::to_string(&checkpoint).unwrap(),
                                        ))
                                        .unwrap();
                                }
                                None => info!(
                                    "no checkpoint available on client subscription for market {}",
                                    &market_id
                                ),
                            };
                        }
                    }
                }
                None => {}
            }
            match cmd.account_ids {
                Some(account_ids) => {
                    wildcard = false;
                    for account_id in account_ids {
                        if peer.account_subscriptions.insert(account_id.clone()) {
                            let res = StatusResponse {
                                success: true,
                                message: &format!("subscribed to account {}", &account_id),
                            };

                            peer.sender
                                .unbounded_send(Message::Text(serde_json::to_string(&res).unwrap()))
                                .unwrap();
                        }
                    }
                }
                None => {}
            }
            if wildcard {
                for (market_id, market_name) in market_ids {
                    if peer.market_subscriptions.insert(market_id.clone()) {
                        let res = StatusResponse {
                            success: true,
                            message: &format!("subscribed to market {}", &market_name),
                        };

                        peer.sender
                            .unbounded_send(Message::Text(serde_json::to_string(&res).unwrap()))
                            .unwrap();
                    }
                }
            }
        }
        Ok(Command::Unsubscribe(cmd)) => {
            info!("unsubscribe {}", cmd.market_id);
            let unsubscribed = peer.market_subscriptions.remove(&cmd.market_id);
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
    let exit: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

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
        0,
    );
    let group_context = Arc::new(
        MangoGroupContext::new_from_rpc(
            &client.rpc_async(),
            Pubkey::from_str(&config.mango_group).unwrap(),
        )
        .await?,
    );

    // todo: reload markets at intervals
    let perp_market_configs: Vec<(Pubkey, MarketConfig)> = group_context
        .perp_markets
        .iter()
        .map(|(_, context)| {
            let quote_decimals = match group_context.tokens.get(&context.market.settle_token_index)
            {
                Some(token) => token.decimals,
                None => panic!("token not found for market"), // todo: default to 6 for usdc?
            };
            (
                context.address,
                MarketConfig {
                    name: context.market.name().to_owned(),
                    bids: context.market.bids,
                    asks: context.market.asks,
                    event_queue: context.market.event_queue,
                    base_decimals: context.market.base_decimals,
                    quote_decimals,
                    base_lot_size: context.market.base_lot_size,
                    quote_lot_size: context.market.quote_lot_size,
                },
            )
        })
        .collect();

    let spot_market_configs: Vec<(Pubkey, MarketConfig)> = group_context
        .serum3_markets
        .iter()
        .map(|(_, context)| {
            let base_decimals = match group_context.tokens.get(&context.market.base_token_index) {
                Some(token) => token.decimals,
                None => panic!("token not found for market"), // todo: default?
            };
            let quote_decimals = match group_context.tokens.get(&context.market.quote_token_index) {
                Some(token) => token.decimals,
                None => panic!("token not found for market"), // todo: default to 6 for usdc?
            };
            (
                context.market.serum_market_external,
                MarketConfig {
                    name: context.market.name().to_owned(),
                    bids: context.bids,
                    asks: context.asks,
                    event_queue: context.event_q,
                    base_decimals,
                    quote_decimals,
                    base_lot_size: context.pc_lot_size as i64,
                    quote_lot_size: context.coin_lot_size as i64,
                },
            )
        })
        .collect();

    let perp_queue_pks: Vec<(Pubkey, Pubkey)> = group_context
        .perp_markets
        .iter()
        .map(|(_, context)| (context.address, context.market.event_queue))
        .collect();

    let _a: Vec<(String, String)> = group_context
        .serum3_markets
        .iter()
        .map(|(_, context)| {
            (
                context.market.serum_market_external.to_string(),
                context.market.name().to_owned(),
            )
        })
        .collect();
    let b: Vec<(String, String)> = group_context
        .perp_markets
        .iter()
        .map(|(_, context)| {
            (
                context.address.to_string(),
                context.market.name().to_owned(),
            )
        })
        .collect();
    let market_pubkey_strings: HashMap<String, String> = [b].concat().into_iter().collect();

    // TODO: read all this from config
    let pgconf = PostgresConfig {
        connection_string: "$PG_CONNECTION_STRING".to_owned(),
        connection_count: 1,
        max_batch_size: 1,
        max_queue_size: 50_000,
        retry_query_max_count: 10,
        retry_query_sleep_secs: 2,
        retry_connection_sleep_secs: 10,
        fatal_connection_timeout_secs: 120,
        allow_invalid_certs: true,
        tls: Some(PostgresTlsConfig {
            ca_cert_path: "$PG_CA_CERT".to_owned(),
            client_key_path: "$PG_CLIENT_KEY".to_owned(),
        }),
    };
    let postgres_update_sender =
        fill_event_postgres_target::init(&pgconf, metrics_tx.clone(), exit.clone()).await?;

    let (account_write_queue_sender, slot_queue_sender, fill_receiver) = fill_event_filter::init(
        perp_market_configs.clone(),
        spot_market_configs.clone(),
        metrics_tx.clone(),
        exit.clone(),
    )
    .await?;

    let checkpoints = CheckpointMap::new(Mutex::new(HashMap::new()));
    let peers = PeerMap::new(Mutex::new(HashMap::new()));

    let checkpoints_ref_thread = checkpoints.clone();
    let peers_ref_thread = peers.clone();
    let peers_ref_thread1 = peers.clone();

    // filleventfilter websocket sink
    tokio::spawn(async move {
        pin!(fill_receiver);
        loop {
            let message = fill_receiver.recv().await.unwrap();
            match message {
                FillEventFilterMessage::Update(update) => {
                    debug!(
                        "ws update {} {:?} {:?} fill",
                        update.market_name, update.status, update.event.event_type
                    );
                    let mut peer_copy = peers_ref_thread.lock().unwrap().clone();
                    for (addr, peer) in peer_copy.iter_mut() {
                        let json = serde_json::to_string(&update.clone()).unwrap();

                        // only send updates if the peer is subscribed
                        if peer.market_subscriptions.contains(&update.market_key) {
                            let result = peer.sender.send(Message::Text(json)).await;
                            if result.is_err() {
                                error!(
                                    "ws update {} fill could not reach {}",
                                    update.market_name, addr
                                );
                            }
                        }
                    }
                    // send fills to db
                    let update_c = update.clone();
                    match update_c.event.event_type {
                        FillEventType::Perp => {
                            postgres_update_sender.send(update_c).await.unwrap();
                        }
                        _ => warn!("failed to write spot event to db"),
                    }
                }
                FillEventFilterMessage::Checkpoint(checkpoint) => {
                    checkpoints_ref_thread
                        .lock()
                        .unwrap()
                        .insert(checkpoint.queue.clone(), checkpoint);
                }
                _ => {}
            }
        }
    });

    // websocket listener
    info!("ws listen: {}", config.bind_ws_addr);
    let try_socket = TcpListener::bind(&config.bind_ws_addr).await;
    let listener = try_socket.expect("Failed to bind");
    {
        tokio::spawn(async move {
            // Let's spawn the handling of each connection in a separate task.
            while let Ok((stream, addr)) = listener.accept().await {
                tokio::spawn(handle_connection_error(
                    checkpoints.clone(),
                    peers.clone(),
                    market_pubkey_strings.clone(),
                    stream,
                    addr,
                    metrics_opened_connections.clone(),
                    metrics_closed_connections.clone(),
                ));
            }
        });
    }

    // keepalive
    {
        tokio::spawn(async move {
            let mut write_interval = time::interval(time::Duration::from_secs(30));

            loop {
                write_interval.tick().await;
                let peers_copy = peers_ref_thread1.lock().unwrap().clone();
                for (addr, peer) in peers_copy.iter() {
                    let pl = Vec::new();
                    let result = peer.clone().sender.send(Message::Ping(pl)).await;
                    if result.is_err() {
                        error!("ws ping could not reach {}", addr);
                    }
                }
            }
        });
    }

    // handle sigint
    {
        let exit = exit.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            info!("Received SIGINT, shutting down...");
            exit.store(true, Ordering::Relaxed);
        });
    }

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
    let all_queue_pks = [perp_queue_pks.clone()].concat();
    let relevant_pubkeys = all_queue_pks.iter().map(|m| m.1.to_string()).collect();
    let filter_config = FilterConfig {
        program_ids: vec![],
        account_ids: relevant_pubkeys,
    };
    if use_geyser {
        grpc_plugin_source::process_events(
            &config.source,
            &filter_config,
            account_write_queue_sender,
            slot_queue_sender,
            metrics_tx.clone(),
            exit.clone(),
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
