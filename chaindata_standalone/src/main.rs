use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::{create_geyser_autoconnection_task};
use itertools::Itertools;
use log::{debug, info, trace, warn};
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;

use tokio::sync::{mpsc, broadcast};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeUpdatePing};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;

use mango_feeds_connector::chain_data::{ChainData, SlotStatus};
use mango_feeds_connector::{AccountWrite, SlotUpdate};
use crate::account_write::account_write_from;
use crate::get_program_account::get_snapshot_gpa;

use crate::router_impl::{AccountOrSnapshotUpdate, spawn_updater_job};

mod router_impl;
mod get_program_account;
mod solana_rpc_minimal;
mod account_write;

pub type ChainDataArcRw = Arc<RwLock<ChainData>>;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
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


    let (account_write_sender, account_write_receiver) = mpsc::channel::<AccountOrSnapshotUpdate>(100_000);
    let (slot_sender, slot_receiver) = async_channel::unbounded::<SlotUpdate>();
    let (account_update_sender, _) = broadcast::channel(524288); // 524288

    start_plumbing_task(grpc_accounts_rx, account_write_sender.clone(), slot_sender.clone());

    let rpc_http_url = env::var("RPC_HTTP_URL").expect("need http rpc url");
    start_gpa_snapshot_fetcher(
        rpc_http_url, Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap(),
        account_write_sender.clone());


    let chain_data = Arc::new(RwLock::new(ChainData::new()));
    let job1 = router_impl::start_chaindata_updating(
        chain_data.clone(),
        account_write_receiver,
        slot_receiver,
        account_update_sender.clone(),
        exit_sender.subscribe(),
    );

    let job2 = spawn_updater_job(
        chain_data.clone(),
        account_update_sender.subscribe(),
        exit_sender.subscribe(),
    );

    info!("chaindata standalone started - now wait some time to let it operate ..");

    let job3 = debug_chaindata(chain_data.clone(), exit_sender.subscribe(),);

    sleep(std::time::Duration::from_secs(100)).await;
    info!("done.");

    info!("send exit signal..");
    exit_sender.send(()).unwrap();
    sleep(std::time::Duration::from_secs(1)).await;
    info!("quitting.");
}

fn start_gpa_snapshot_fetcher(rpc_http_url: String, program_id: Pubkey, account_write_sender: mpsc::Sender<AccountOrSnapshotUpdate>) {
    tokio::spawn(async move {
        info!("loading snapshot from compressed gPA RPC endpoint ...");
        let rpc_http_url = rpc_http_url.clone();
        // 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 -> 796157 accounts, 50s
        let snapshot = get_snapshot_gpa(&rpc_http_url, &program_id, true).await.unwrap();
        info!("downloaded snapshot for slot {} with {:?} accounts", snapshot.slot, snapshot.accounts.len());

        for update_chunk in snapshot.accounts.chunks(1024) {
            let chunk = update_chunk.into_iter()
                .map(|update| {
                    let slot = update.slot;
                    let pubkey = Pubkey::try_from(update.pubkey.clone()).unwrap();
                    AccountWrite {
                        pubkey,
                        slot,
                        write_version: update.write_version,
                        lamports: update.lamports,
                        owner: Pubkey::try_from(update.owner.clone()).unwrap(),
                        executable: update.executable,
                        rent_epoch: update.rent_epoch,
                        data: update.data.clone(), // TODO not nice
                        is_selected: false, // what is that?
                    }
                }).collect_vec();

            info!("sending snapshot chunk with {} accounts", chunk.len());
            let _sent_res = account_write_sender.send(AccountOrSnapshotUpdate::SnapshotUpdate(chunk)).await;
        }

    });
}

// TODO add exit
fn debug_chaindata(chain_data: Arc<RwLock<ChainData>>, mut exit: broadcast::Receiver<()>,) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting debug task");
        loop {
            if exit.try_recv().is_ok() {
                info!("exit signal received - stopping task");
                return;
            }
            {
                let chain_data = chain_data.read().unwrap();
                info!("chaindata?");
                for account in chain_data.iter_accounts_rooted() {
                    info!("- chaindata.account {:?}", account);
                }
            }

            sleep(std::time::Duration::from_secs(1)).await;
        }
    })
}

// this is replacing the spawn_geyser_source task from router
fn start_plumbing_task(
    mut grpc_source_rx: mpsc::Receiver<Message>,
    account_write_sender: mpsc::Sender<AccountOrSnapshotUpdate>,
    slot_sender: async_channel::Sender<SlotUpdate>) {
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

                        trace!("[grpc->account_write_sender]: account update for {}@_slot_{} write_version={}",
                            pubkey, slot, update.write_version);

                        account_write_sender.send(AccountOrSnapshotUpdate::AccountUpdate(AccountWrite {
                            pubkey,
                            slot,
                            write_version: update.write_version,
                            lamports: update.lamports,
                            owner,
                            executable: update.executable,
                            rent_epoch: update.rent_epoch,
                            data: update.data,
                            is_selected: false, // what is that?
                        })).await.expect("channel account_write_sender must not be closed");
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
        commitment: Some(CommitmentLevel::Processed as i32), // default
        ..Default::default()
    }
}

