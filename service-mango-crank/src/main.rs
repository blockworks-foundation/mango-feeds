mod blockhash_poller;
mod transaction_builder;
mod transaction_sender;

use anchor_client::{
    solana_sdk::{account::Account, commitment_config::CommitmentConfig, signature::Keypair},
    Cluster,
};
use anchor_lang::prelude::Pubkey;
use bytemuck::cast_slice;
use client::{Client, MangoGroupContext};
use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{
    future::{self, Ready},
    pin_mut, SinkExt, StreamExt, TryStreamExt,
};
use log::*;
use solana_client::{nonblocking::blockhash_query, nonblocking::rpc_client::RpcClient};
use std::{
    collections::{HashMap, HashSet},
    convert::identity,
    fs::File,
    io::Read,
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    sync::{atomic::AtomicBool, Mutex},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    pin, time,
};
use tokio_tungstenite::tungstenite::{protocol::Message, Error};

use serde::Deserialize;
use solana_geyser_connector_lib::{
    fill_event_filter::{self, FillCheckpoint},
    grpc_plugin_source, metrics, websocket_source, MetricsConfig, SourceConfig,
};
use solana_geyser_connector_lib::{
    metrics::{MetricType, MetricU64},
    FilterConfig, StatusResponse,
};

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
    solana_logger::setup_with_default("info");

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        error!("Please enter a config file path argument.");
        return Ok(());
    }

    let config: Config = {
        let mut file = File::open(&args[1])?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        toml::from_str(&contents).unwrap()
    };

    let rpc_client = Arc::new(RpcClient::new(config.rpc_http_url.clone()));

    let blockhash = blockhash_poller::init(rpc_client.clone()).await;

    let metrics_tx = metrics::start(config.metrics, "crank".into());

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
    let group_pk = Pubkey::from_str(&config.mango_group).unwrap();
    let group_context = Arc::new(
        MangoGroupContext::new_from_rpc(
            &client.rpc_async(),
            group_pk
        )
        .await?,
    );

    let perp_queue_pks: Vec<(Pubkey, Pubkey)> = group_context
        .perp_markets
        .iter()
        .map(|(_, context)| (context.address, context.market.event_queue))
        .collect();

    // fetch all serum/openbook markets to find their event queues
    let serum_market_pks: Vec<Pubkey> = group_context
        .serum3_markets
        .iter()
        .map(|(_, context)| context.market.serum_market_external)
        .collect();

    let serum_market_ais = client
        .rpc_async()
        .get_multiple_accounts(serum_market_pks.as_slice())
        .await?;

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
            (
                serum_market_pks[pair.0],
                Pubkey::new(cast_slice(&identity(market_state.event_q) as &[_])),
            )
        })
        .collect();

    let (account_write_queue_sender, slot_queue_sender, instruction_receiver) =
        transaction_builder::init(
            perp_queue_pks.clone(),
            serum_queue_pks.clone(),
            group_pk,
            metrics_tx.clone(),
        )
        .expect("init transaction builder");

    transaction_sender::init(instruction_receiver, blockhash, rpc_client, Keypair::new());

    info!(
        "connect: {}",
        config
            .source
            .grpc_sources
            .iter()
            .map(|c| c.connection_string.clone())
            .collect::<String>()
    );
    let use_geyser = true;
    let all_queue_pks = [perp_queue_pks.clone(), serum_queue_pks.clone()].concat();
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
