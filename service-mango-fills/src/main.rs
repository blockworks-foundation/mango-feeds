use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{pin_mut, SinkExt, StreamExt};
use log::*;
use std::{collections::HashMap, fs::File, io::Read, net::SocketAddr, sync::Arc, sync::Mutex};
use tokio::{
    net::{TcpListener, TcpStream},
    pin,
};
use tokio_tungstenite::tungstenite::{protocol::Message, Error};

use serde::Deserialize;
use solana_geyser_connector_lib::metrics::MetricU64;
use solana_geyser_connector_lib::{
    fill_event_filter::{self, FillCheckpoint, FillEventFilterMessage, MarketConfig},
    grpc_plugin_source, metrics, websocket_source, SourceConfig,
};

use crate::metrics::Metrics;
use warp::{Filter, Rejection, Reply};
type CheckpointMap = Arc<Mutex<HashMap<String, FillCheckpoint>>>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, UnboundedSender<Message>>>>;

async fn handle_connection_error(
    checkpoint_map: CheckpointMap,
    peer_map: PeerMap,
    raw_stream: TcpStream,
    addr: SocketAddr,
    metrics_opened_connections: MetricU64,
    metrics_closed_connections: MetricU64,
) {
    metrics_opened_connections.clone().increment();

    let result = handle_connection(checkpoint_map, peer_map.clone(), raw_stream, addr).await;
    if result.is_err() {
        error!("connection {} error {}", addr, result.unwrap_err());
    };

    metrics_closed_connections.clone().increment();

    peer_map.lock().unwrap().remove(&addr);
}

async fn handle_connection(
    checkpoint_map: CheckpointMap,
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

async fn handle_metrics(metrics: Metrics) -> Result<impl Reply, Rejection> {
    info!("handle_metrics");
    let labels = HashMap::from([("process", "fills")]);
    let label_strings_vec: Vec<String> = labels
        .iter()
        .map(|(name, value)| format!("{}=\"{}\"", name, value))
        .collect();
    let lines: Vec<String> = metrics
        .get_registry_vec()
        .iter()
        .map(|(name, value)| format!("{}{{{}}} {}", name, label_strings_vec.join(","), value))
        .collect();
    Ok(lines.join("\n"))
}

pub fn with_metrics(
    metrics: Metrics,
) -> impl Filter<Extract = (Metrics,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || metrics.clone())
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub markets: Vec<MarketConfig>,
    pub bind_ws_addr: String,
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

    let metrics_tx = metrics::start();
    let metrics_route = warp::path!("metrics")
        .and(with_metrics(metrics_tx.clone()))
        .and_then(handle_metrics);

    // serve prometheus metrics endpoint
    tokio::spawn(async move {
        warp::serve(metrics_route).run(([0, 0, 0, 0], 9091)).await;
    });

    let metrics_opened_connections =
        metrics_tx.register_u64("fills_feed_opened_connections".into());

    let metrics_closed_connections =
        metrics_tx.register_u64("fills_feed_closed_connections".into());

    let (account_write_queue_sender, slot_queue_sender, fill_receiver) =
        fill_event_filter::init(config.markets.clone(), metrics_tx.clone()).await?;

    let checkpoints = CheckpointMap::new(Mutex::new(HashMap::new()));
    let peers = PeerMap::new(Mutex::new(HashMap::new()));

    let checkpoints_ref_thread = checkpoints.clone();
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
    if use_geyser {
        grpc_plugin_source::process_events(
            &config.source,
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
