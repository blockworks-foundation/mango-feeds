use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::time::Duration;
use async_channel::Sender;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::{create_geyser_autoconnection_task, create_geyser_autoconnection_task_with_mpsc};
use log::{debug, info, trace, warn};
use solana_sdk::pubkey::Pubkey;

use tokio::sync::broadcast;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestPing, SubscribeUpdatePing, SubscribeUpdatePong};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;

use mango_feeds_connector::chain_data::{ChainData, SlotStatus};
use mango_feeds_connector::{AccountWrite, SlotUpdate};

mod router_impl;

pub type ChainDataArcRw = Arc<RwLock<ChainData>>;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let (exit_sender, _) = broadcast::channel(1);

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(15),
        request_timeout: Duration::from_secs(15),
        subscribe_timeout: Duration::from_secs(15),
        receive_timeout: Duration::from_secs(15),
    };

    let grpc_addr = env::var("GRPC_ADDR").expect("need grpc url");
    let grpc_x_token = env::var("GRPC_X_TOKEN").ok();

    let grpc_source_config = GrpcSourceConfig::new(grpc_addr, grpc_x_token, None, timeouts.clone());

    let (_jh_grpc_source, grpc_accounts_rx) = create_geyser_autoconnection_task(grpc_source_config.clone(), raydium_accounts(), exit_sender.subscribe());


    let (account_write_sender, account_write_receiver) = async_channel::unbounded::<AccountWrite>();
    let (slot_sender, slot_receiver) = async_channel::unbounded::<SlotUpdate>();
    let (account_update_sender, _) = broadcast::channel(524288);

    // TODO exit
    start_plumbing_task(grpc_accounts_rx, account_write_sender.clone(), slot_sender.clone());

    let chain_data = Arc::new(RwLock::new(ChainData::new()));
    router_impl::start_chaindata_updating(
        chain_data.clone(),
        account_write_receiver,
        slot_receiver,
        account_update_sender.clone(),
        exit_sender.subscribe(),
    );

    info!("chaindata standalone started - now wait some time to let it operate ..");

    let jh_debug = debug_chaindata(chain_data.clone());

    sleep(std::time::Duration::from_secs(7));
    info!("done.");

    jh_debug.abort();
    info!("send exit signal..");
    exit_sender.send(()).unwrap();
    sleep(std::time::Duration::from_secs(1));
    info!("quitting.");
}

// TODO add exit
fn debug_chaindata(chain_data: Arc<RwLock<ChainData>>) -> JoinHandle<()> {

    let jh_debug = tokio::spawn(async move {
        info!("starting debug task");
        loop {
            let chain_data = chain_data.read().unwrap();
            info!("chaindata?");
            for account in chain_data.iter_accounts_rooted() {
                info!("- chaindata.account {:?}", account);
            }
            sleep(std::time::Duration::from_secs(1));
        }
    });

    jh_debug
}

fn start_plumbing_task(
    mut grpc_source_rx: Receiver<Message>,
    account_write_sender: Sender<AccountWrite>,
    slot_sender: Sender<SlotUpdate>) {
    tokio::spawn(async move {
        info!("starting plumbing task");
        loop {
            if let Some(Message::GeyserSubscribeUpdate(subscribe_update)) = grpc_source_rx.recv().await {
                match subscribe_update.update_oneof {
                    Some(UpdateOneof::Account(account_update)) => {
                        let slot = account_update.slot;
                        let update = account_update.account.unwrap();
                        let pubkey = Pubkey::try_from(update.pubkey.clone()).unwrap();
                        let owner = Pubkey::try_from(update.owner.clone()).unwrap();

                        trace!("get account update for {:?}@{} via grpc", pubkey, slot);

                        account_write_sender.send(AccountWrite {
                            pubkey,
                            slot,
                            write_version: update.write_version,
                            lamports: update.lamports,
                            owner,
                            executable: update.executable,
                            rent_epoch: update.rent_epoch,
                            data: update.data,
                            is_selected: false, // what is that?
                        }).await.expect("channel account_write_sender must not be closed");
                    }
                    Some(UpdateOneof::Slot(slot_update)) => {
                        let slot = slot_update.slot;
                        let parent = slot_update.parent;
                        let status = CommitmentLevel::try_from(slot_update.status).map(|v| match v {
                            CommitmentLevel::Processed => SlotStatus::Processed,
                            CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                            CommitmentLevel::Finalized => SlotStatus::Rooted,
                        }).expect("valid commitment level");
                        trace!("get slot update {:?} -> {}", parent, slot);
                        
                        slot_sender.send(SlotUpdate { slot, parent, status }).await
                            .expect("channel slot_sender must not be closed");
                    }
                    Some(UpdateOneof::Ping(SubscribeUpdatePing {})) => {
                        debug!("grpc geyser ping");
                    }
                    _ => {
                        warn!("other update (unexpected)")
                    }
                }
            } else {
                warn!("grpc source rx closed - stopping task");
                return;
            }
        }
    });

}


fn raydium_accounts() -> SubscribeRequest {
    let raydium_amm_pubkey = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string();

    let mut accounts_subs = HashMap::new();
    accounts_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![raydium_amm_pubkey],
            filters: vec![],
        },
    );

    SubscribeRequest {
        accounts: accounts_subs,
        ping: None,
        ..Default::default()
    }
}

