mod orderbook_filter;

use anchor_client::{
    solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair},
    Cluster,
};
use anchor_lang::prelude::Pubkey;
use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{
    future::{self, Ready},
    pin_mut, SinkExt, StreamExt, TryStreamExt,
};
use itertools::Itertools;
use log::*;
use mango_v4_client::{Client, MangoGroupContext, TransactionBuilderConfig};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Read,
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    pin, time,
};
use tokio_tungstenite::tungstenite::{protocol::Message, Error};

use mango_feeds_lib::{
    grpc_plugin_source, metrics, websocket_source, MarketConfig, MetricsConfig, SourceConfig,
};
use mango_feeds_lib::{
    metrics::{MetricType, MetricU64},
    FilterConfig, StatusResponse,
};
use serde::Deserialize;

use service_mango_orderbook::{OrderbookCheckpoint, OrderbookFilterMessage};

type CheckpointMap = Arc<Mutex<HashMap<String, OrderbookCheckpoint>>>;
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

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub metrics: MetricsConfig,
    pub bind_ws_addr: String,
    pub rpc_http_url: String,
    pub mango_group: String,
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
                subscriptions: HashSet::<String>::new(),
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
    let msg_str = msg.into_text().unwrap();
    let command: Result<Command, serde_json::Error> = serde_json::from_str(&msg_str);
    let mut peers = peer_map.lock().unwrap();
    let peer = peers.get_mut(&addr).expect("peer should be in map");
    match command {
        Ok(Command::Subscribe(cmd)) => {
            let market_id = cmd.market_id;
            if market_ids.get(&market_id).is_none() {
                let res = StatusResponse {
                    success: false,
                    message: "market not found",
                };
                peer.sender
                    .unbounded_send(Message::Text(serde_json::to_string(&res).unwrap()))
                    .unwrap();
                return future::ok(());
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
                    None => info!("no checkpoint available on client subscription"), // todo: what to do here?
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    solana_logger::setup_with_default("info");
    let exit: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

    // load config
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Please enter a config file path argument");
        return Ok(());
    }
    let config: Config = {
        let mut file = File::open(&args[1])?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        toml::from_str(&contents).unwrap()
    };

    // setup metrics
    let metrics_tx = metrics::start(config.metrics, "orderbook".into());
    let metrics_opened_connections =
        metrics_tx.register_u64("orderbook_opened_connections".into(), MetricType::Counter);
    let metrics_closed_connections =
        metrics_tx.register_u64("orderbook_closed_connections".into(), MetricType::Counter);

    // load mango group and markets from rpc
    let rpc_url = config.rpc_http_url;
    let ws_url = rpc_url.replace("https", "wss");
    let rpc_timeout = Duration::from_secs(10);
    let cluster = Cluster::Custom(rpc_url.clone(), ws_url.clone());
    let client = Client::new(
        cluster.clone(),
        CommitmentConfig::processed(),
        Arc::new(Keypair::new()),
        Some(rpc_timeout),
        TransactionBuilderConfig {
            prioritization_micro_lamports: None,
        },
    );
    let group_context = Arc::new(
        MangoGroupContext::new_from_rpc(
            &client.rpc_async(),
            Pubkey::from_str(&config.mango_group).unwrap(),
        )
        .await?,
    );

    // todo: reload markets at intervals
    let market_configs: Vec<(Pubkey, MarketConfig)> = group_context
        .perp_markets
        .values()
        .map(|context| {
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
                    oracle: context.market.oracle,
                    base_decimals: context.market.base_decimals,
                    quote_decimals,
                    base_lot_size: context.market.base_lot_size,
                    quote_lot_size: context.market.quote_lot_size,
                },
            )
        })
        .collect();

    let serum_market_configs: Vec<(Pubkey, MarketConfig)> = group_context
        .serum3_markets
        .values()
        .map(|context| {
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
                    oracle: Pubkey::default(), // serum markets dont support oracle peg
                    base_decimals,
                    quote_decimals,
                    base_lot_size: context.coin_lot_size as i64,
                    quote_lot_size: context.pc_lot_size as i64,
                },
            )
        })
        .collect();

    let market_pubkey_strings: HashMap<String, String> =
        [market_configs.clone(), serum_market_configs.clone()]
            .concat()
            .iter()
            .map(|market| (market.0.to_string(), market.1.name.clone()))
            .collect::<Vec<(String, String)>>()
            .into_iter()
            .collect();

    let (account_write_queue_sender, slot_queue_sender, orderbook_receiver) =
        orderbook_filter::init(
            market_configs.clone(),
            serum_market_configs.clone(),
            metrics_tx.clone(),
            exit.clone(),
        )
        .await?;

    let checkpoints = CheckpointMap::new(Mutex::new(HashMap::new()));
    let peers = PeerMap::new(Mutex::new(HashMap::new()));

    // orderbook receiver
    {
        let checkpoints = checkpoints.clone();
        let peers = peers.clone();
        let exit = exit.clone();
        tokio::spawn(async move {
            pin!(orderbook_receiver);
            loop {
                if exit.load(Ordering::Relaxed) {
                    warn!("shutting down orderbook receiver...");
                    break;
                }

                let message: OrderbookFilterMessage = orderbook_receiver.recv().await.unwrap();
                match message {
                    OrderbookFilterMessage::Update(update) => {
                        debug!("ws update {} {:?}", update.market, update.side);
                        let mut peer_copy = peers.lock().unwrap().clone();
                        for (addr, peer) in peer_copy.iter_mut() {
                            let json = serde_json::to_string(&update).unwrap();

                            // only send updates if the peer is subscribed
                            if peer.subscriptions.contains(&update.market) {
                                let result = peer.sender.send(Message::Text(json)).await;
                                if result.is_err() {
                                    error!(
                                        "ws update {} {:?} could not reach {}",
                                        update.market, update.side, addr
                                    );
                                }
                            }
                        }
                    }
                    OrderbookFilterMessage::Checkpoint(checkpoint) => {
                        checkpoints
                            .lock()
                            .unwrap()
                            .insert(checkpoint.market.clone(), checkpoint);
                    }
                }
            }
        });
    }

    // websocket server
    {
        info!("ws listen: {}", config.bind_ws_addr);
        let try_socket = TcpListener::bind(&config.bind_ws_addr).await;
        let listener = try_socket.expect("Failed to bind");
        let exit = exit.clone();
        let peers = peers.clone();
        tokio::spawn(async move {
            // Let's spawn the handling of each connection in a separate task.
            while let Ok((stream, addr)) = listener.accept().await {
                if exit.load(Ordering::Relaxed) {
                    warn!("shutting down websocket server...");
                    break;
                }
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
        let exit = exit.clone();
        let peers = peers.clone();
        tokio::spawn(async move {
            let mut write_interval = time::interval(time::Duration::from_secs(30));

            loop {
                if exit.load(Ordering::Relaxed) {
                    warn!("shutting down keepalive...");
                    break;
                }

                write_interval.tick().await;
                let peers_copy = peers.lock().unwrap().clone();
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

    let relevant_pubkeys = [market_configs.clone(), serum_market_configs.clone()]
        .concat()
        .iter()
        .flat_map(|m| [m.1.bids.to_string(), m.1.asks.to_string()])
        .collect_vec();
    let filter_config = FilterConfig {
        program_ids: vec![],
        account_ids: [
            relevant_pubkeys,
            market_configs
                .iter()
                .map(|(_, mkt)| mkt.oracle.to_string())
                .collect_vec(),
        ]
        .concat()
        .to_vec(),
    };
    let use_geyser = true;
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
            &filter_config,
            account_write_queue_sender,
            slot_queue_sender,
        )
        .await;
    }

    Ok(())
}
