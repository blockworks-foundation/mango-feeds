use std::collections::HashMap;
use std::env;
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::time::Duration;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::{create_geyser_autoconnection_task, create_geyser_autoconnection_task_with_mpsc};
use log::{info, warn};

use tokio::sync::broadcast;
use tokio::sync::mpsc::Receiver;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterAccounts};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;

use mango_feeds_connector::chain_data::ChainData;
use mango_feeds_connector::{AccountWrite, SlotUpdate};

mod router_impl;

pub type ChainDataArcRw = Arc<RwLock<ChainData>>;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let (exit_sender, _) = broadcast::channel(1);

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    let grpc_addr = env::var("GRPC_ADDR").expect("need grpc url");
    let grpc_x_token = env::var("GRPC_X_TOKEN").ok();

    let grpc_source_config = GrpcSourceConfig::new(grpc_addr, grpc_x_token, None, timeouts.clone());

    let (_jh_grpc_source, grpc_accounts_rx) = create_geyser_autoconnection_task(grpc_source_config.clone(), all_accounts(), exit_sender.subscribe());


    let (account_write_sender, account_write_receiver) = async_channel::unbounded::<AccountWrite>();
    let (slot_sender, slot_receiver) = async_channel::unbounded::<SlotUpdate>();
    let (account_update_sender, _) = broadcast::channel(524288);

    start_connector_task(grpc_accounts_rx);

    let chain_data = Arc::new(RwLock::new(ChainData::new()));
    router_impl::start_chaindata_updating(
        chain_data.clone(),
        account_write_receiver,
        slot_receiver,
        account_update_sender.clone(),
        exit_sender.subscribe(),
    );

    info!("chaindata standalone started - now wait some time to let it operate ..");
    sleep(std::time::Duration::from_secs(3));
    info!("send exit signal..");
    exit_sender.send(()).unwrap();
    sleep(std::time::Duration::from_secs(1));
    info!("quitting.");
}

fn start_connector_task(mut grpc_source_rx: Receiver<Message>) {
    tokio::spawn(async move {
        loop {
            if let Some(Message::GeyserSubscribeUpdate(subscribe_update)) = grpc_source_rx.recv().await {
                match subscribe_update.update_oneof {
                    Some(UpdateOneof::Account(accout_update)) => {
                        info!("account update")
                    }
                    _ => {
                        warn!("other update (unexpected)")
                    }
                }
            }
        }
    });

}


fn all_accounts() -> SubscribeRequest {
    let mut accounts_subs = HashMap::new();
    accounts_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![],
            filters: vec![],
        },
    );

    SubscribeRequest {
        accounts: accounts_subs,
        ..Default::default()
    }
}

