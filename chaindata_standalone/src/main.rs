use std::collections::HashMap;
use std::env;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::mpsc::SendError;
use std::time::Duration;
use geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::{create_geyser_autoconnection_task, create_geyser_autoconnection_task_with_mpsc};
use itertools::Itertools;
use log::{debug, info, trace, warn};
use prometheus::{IntCounter, register_int_counter};
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;

use tokio::sync::{mpsc, broadcast};
use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots, SubscribeUpdatePing};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;

use mango_feeds_connector::chain_data::{ChainData, SlotStatus};
use mango_feeds_connector::{AccountWrite, SlotUpdate};
use crate::account_write::account_write_from;
use crate::get_program_account::get_snapshot_gpa;
use crate::metrics_dump::start_metrics_dumper;

use crate::router_impl::{AccountOrSnapshotUpdate, spawn_updater_job};

mod router_impl;
mod get_program_account;
mod solana_rpc_minimal;
mod account_write;
mod metrics_dump;
mod metrics;



pub type ChainDataArcRw = Arc<RwLock<ChainData>>;

// 796157 accounts
const RAYDIUM_AMM_PUBKEY: &'static str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
// 182264 accounts
const WHIRLPOOL_PUBKEY: &'static str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
// 580 accounts
const CROPPER_PUBKEY: &'static str = "H8W3ctz92svYg6mkn1UtGfu2aQr2fnUFHM1RhScEtQDt";
const DEX_PROGRAM_ID: &'static str = CROPPER_PUBKEY;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();

    // start_metrics_dumper(SOME_METRIC.deref());
    start_metrics_dumper(metrics::SNAPSHOT_ACCOUNTS_CNT.deref());
    start_metrics_dumper(metrics::GRPC_PLUMBING_ACCOUNT_MESSAGE_CNT.deref());
    start_metrics_dumper(metrics::GRPC_PLUMBING_SLOT_MESSAGE_CNT.deref());
    start_metrics_dumper(metrics::CHAINDATA_ACCOUNT_WRITE_IN.deref());
    start_metrics_dumper(metrics::CHAINDATA_SLOT_UPDATE_IN.deref());
    start_metrics_dumper(metrics::CHAINDATA_UPDATE_ACCOUNT.deref());
    start_metrics_dumper(metrics::CHAINDATA_SNAP_UPDATE_ACCOUNT.deref());
    start_metrics_dumper(metrics::ACCOUNT_UPDATE_SENDER.deref());

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

    let (grpc_accounts_tx, grpc_accounts_rx) = mpsc::channel(10240);
    let _jh_grpc_source = create_geyser_autoconnection_task_with_mpsc(
        grpc_source_config.clone(), raydium_accounts(), grpc_accounts_tx, exit_sender.subscribe());


    let (account_write_sender, account_write_receiver) = mpsc::channel::<AccountOrSnapshotUpdate>(100_000);
    let (slot_sender, slot_receiver) = mpsc::channel::<SlotUpdate>(10_000);
    let (account_update_sender, _) = broadcast::channel(524288); // 524288

    start_plumbing_task(grpc_accounts_rx, account_write_sender.clone(), slot_sender.clone());

    let rpc_http_url = env::var("RPC_HTTP_URL").expect("need http rpc url");
    start_gpa_snapshot_fetcher(
        rpc_http_url, Pubkey::from_str(DEX_PROGRAM_ID).unwrap(),
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

    sleep(std::time::Duration::from_secs(300)).await;
    info!("done.");

    info!("send exit signal..");
    exit_sender.send(()).unwrap();
    sleep(std::time::Duration::from_secs(1)).await;
    info!("quitting.");
}

fn start_gpa_snapshot_fetcher(rpc_http_url: String, program_id: Pubkey, account_write_sender: mpsc::Sender<AccountOrSnapshotUpdate>) {
    tokio::spawn(async move {
        for i_download in 1.. {
            info!("loading snapshot #{i_download} from compressed gPA RPC endpoint ...");
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

                metrics::SNAPSHOT_ACCOUNTS_CNT.inc_by(chunk.len() as u64);

                info!("sending snapshot chunk with {} accounts", chunk.len());
                if let Err(SendTimeoutError::Timeout(item)) =
                    account_write_sender.send_timeout(AccountOrSnapshotUpdate::SnapshotUpdate(chunk), Duration::from_millis(500)).await {
                    debug!("send to account_write_sender was blocked - retrying");
                    let _res = account_write_sender.send(item).await.unwrap();
                }
                // TODO error handling
            }

            info!("waiting before next snapshot being loaded");
            sleep(std::time::Duration::from_secs(60)).await;
        } // -- endless for loop over snapshots

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
            let account_cnt_before = {
                let chain_data = chain_data.read().unwrap();
                let cnt = chain_data.accounts_count();
                info!("chaindata: {} accounts", cnt);
                cnt
            };
            let mearure_duration = std::time::Duration::from_millis(100);
            sleep(mearure_duration).await;
            let account_cnt_after = {
                let chain_data = chain_data.read().unwrap();
                let cnt = chain_data.accounts_count();
                info!("chaindata: {} accounts", cnt);
                cnt
            };
            // TODO trace timestamps
            info!("chaindata: {} accounts (throughput {} acc/s)", account_cnt_after, (account_cnt_after - account_cnt_before) as f64 / mearure_duration.as_secs_f64());

            sleep(std::time::Duration::from_secs(1)).await;
        }
    })
}

// this is replacing the spawn_geyser_source task from router
fn start_plumbing_task(
    mut grpc_source_rx: mpsc::Receiver<Message>,
    account_write_sender: mpsc::Sender<AccountOrSnapshotUpdate>,
    slot_sender: mpsc::Sender<SlotUpdate>) {
    tokio::spawn(async move {
        info!("starting plumbing task");
        loop {
            if let Some(Message::GeyserSubscribeUpdate(subscribe_update)) = grpc_source_rx.recv().await {
                match subscribe_update.update_oneof {
                    Some(UpdateOneof::Account(account_update)) => {
                        metrics::GRPC_PLUMBING_ACCOUNT_MESSAGE_CNT.inc();
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
                        metrics::GRPC_PLUMBING_SLOT_MESSAGE_CNT.inc();
                        let slot = slot_update.slot;
                        let parent = slot_update.parent;
                        let status = CommitmentLevel::try_from(slot_update.status).map(|v| match v {
                            CommitmentLevel::Processed => SlotStatus::Processed,
                            CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                            CommitmentLevel::Finalized => SlotStatus::Rooted,
                        }).expect("valid commitment level");

                        trace!("[grpc->slot_sender]: slot info for {}@{:?}", slot, status);

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
    let mut slot_subs = HashMap::new();
    slot_subs.insert("client".to_string(), SubscribeRequestFilterSlots {
        filter_by_commitment: None,
    });
    let mut accounts_subs = HashMap::new();
    accounts_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![DEX_PROGRAM_ID.to_string()],
            filters: vec![],
        },
    );

    SubscribeRequest {
        slots: slot_subs,
        accounts: accounts_subs,
        ping: None,
        commitment: None,
        ..Default::default()
    }
}

