use log::{info, trace, warn};
use mango_feeds_connector::chain_data::ChainData;
use mango_feeds_connector::{AccountWrite, SlotUpdate};
use solana_sdk::pubkey::Pubkey;
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant};
use solana_sdk::account::ReadableAccount;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error};
use tracing::field::debug;

pub type ChainDataArcRw = Arc<RwLock<ChainData>>;


#[derive(Debug)]
pub enum AccountOrSnapshotUpdate {
    AccountUpdate(AccountWrite),
    SnapshotUpdate(Vec<AccountWrite>),
}


// from router project
pub fn start_chaindata_updating(
    chain_data: ChainDataArcRw,
    // = account_write_receiver
    mut account_writes: mpsc::Receiver<AccountOrSnapshotUpdate>,
    slot_updates: async_channel::Receiver<SlotUpdate>,
    account_update_sender: broadcast::Sender<(Pubkey, u64)>,
    mut exit: broadcast::Receiver<()>,
) -> JoinHandle<()> {
    use mango_feeds_connector::chain_data::SlotData;

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = exit.recv() => {
                    info!("shutting down chaindata update task");
                    break;
                }
                res = account_writes.recv() => {
                    let Some(update) = res
                    else {
                        warn!("account write channel err {res:?}");
                        continue;
                    };

                    match &update {
                        AccountOrSnapshotUpdate::AccountUpdate(account_update) => {
                            trace!("[account_write_receiver->chain_data] account update for {}@_slot_{} write_version={}",
                                account_update.pubkey, account_update.slot, account_update.write_version);
                        }
                        AccountOrSnapshotUpdate::SnapshotUpdate(account_writes) => {
                            trace!("[account_write_receiver->chain_data] account update from snapshot with {} accounts",
                                account_writes.len());
                        }
                    }

                    let mut writer = chain_data.write().unwrap();
                    handle_updated_account(&mut writer, update, &account_update_sender);

                    let mut batchsize: u32 = 0;
                    let started_at = Instant::now();
                    'batch_loop: while let Ok(bupdate) = account_writes.try_recv() {
                        batchsize += 1;

                        handle_updated_account(&mut writer, bupdate, &account_update_sender);

                        // budget for microbatch
                        if batchsize > 10 || started_at.elapsed() > Duration::from_micros(500) {
                            break 'batch_loop;
                        }
                    }
                }
                res = slot_updates.recv() => {
                    let Ok(slot_update) = res
                    else {
                        warn!("slot channel err {res:?}");
                        continue;
                    };

                    chain_data.write().unwrap().update_slot(SlotData {
                        slot: slot_update.slot,
                        parent: slot_update.parent,
                        status: slot_update.status,
                        chain: 0,
                    });
                }
            }
        }
    })
}

// from router project

// trace!("[account_write_receiver->chain_data] .update_account for {}@_slot_{} write_version={}",
//     update.pubkey, update.slot, update.write_version);


#[tracing::instrument(skip_all, level = "trace")]
fn handle_updated_account(
    chain_data: &mut RwLockWriteGuard<ChainData>,
    update: AccountOrSnapshotUpdate,
    account_update_sender: &broadcast::Sender<(Pubkey, u64)>,
) {
    use mango_feeds_connector::chain_data::AccountData;
    use solana_sdk::account::WritableAccount;
    use solana_sdk::clock::Epoch;

    fn one_update(
        chain_data: &mut RwLockWriteGuard<ChainData>,
        account_update_sender: &broadcast::Sender<(Pubkey, u64)>,
        account_write: AccountWrite,
    ) {
        chain_data.update_account(
            account_write.pubkey,
            AccountData {
                slot: account_write.slot,
                write_version: account_write.write_version,
                account: WritableAccount::create(
                    account_write.lamports,
                    account_write.data,
                    account_write.owner,
                    account_write.executable,
                    account_write.rent_epoch as Epoch,
                ),
            },
        );

        // trace!("[account_write_receiver->account_update_sender] send write for {}@_slot_{} write_version={}",
        // account_write.pubkey, account_write.slot, account_write.write_version);

        // ignore failing sends when there are no receivers
        let _err = account_update_sender.send((account_write.pubkey, account_write.slot));
    }

    fn is_same(account_data: &AccountData, account_write: &AccountWrite) -> bool {
        account_data.slot == account_write.slot
            && account_data.account.lamports() == account_write.lamports
            && account_data.account.owner() == &account_write.owner
            && account_data.account.data() == account_write.data
    }


    match update {
        AccountOrSnapshotUpdate::AccountUpdate(account_write) => {
            trace!("[one_update] Update from single account grpc: {}@_slot_{} write_version={}",
                account_write.pubkey, account_write.slot, account_write.write_version);
            one_update(chain_data, account_update_sender, account_write);
        }
        AccountOrSnapshotUpdate::SnapshotUpdate(snapshot) => {
            debug!("Update from snapshot data: {}", snapshot.len());
            let mut fixed_cnt: usize = 0;
            for snap_account_write in snapshot {
                let pubkey = snap_account_write.pubkey;
                let current_account_data = chain_data.account(&pubkey);
                if let Ok(current_account_data) = current_account_data {
                    if snap_account_write.slot >= current_account_data.slot {
                        if !is_same(current_account_data, &snap_account_write) {
                            trace!("Fixed chain_data from snapshot data, account {}, slot {}->{}",
                                pubkey, current_account_data.slot, snap_account_write.slot);
                            fixed_cnt += 1;
                        }
                    }
                }

                trace!("[one_update] Update from snap account data: {}@_slot_{} write_version={}",
                    snap_account_write.pubkey, snap_account_write.slot, snap_account_write.write_version);
                one_update(chain_data, account_update_sender, snap_account_write);
            }

            info!("Fixed {} accounts from snapshot data", fixed_cnt);
        }
    }
}



pub fn spawn_updater_job(
    chain_data: ChainDataArcRw,
    mut account_updates: broadcast::Receiver<(Pubkey, u64)>,
    mut exit: broadcast::Receiver<()>,
) -> JoinHandle<()> {

    let listener_job = tokio::spawn(async move {

        let mut refresh_all_interval = tokio::time::interval(Duration::from_secs(1));
        let mut refresh_one_interval = tokio::time::interval(Duration::from_millis(10));
        refresh_all_interval.tick().await;
        refresh_one_interval.tick().await;

        'drain_loop: loop {
            tokio::select! {
                _ = exit.recv() => {
                    info!("shutting down update task");
                    break;
                }
                res = account_updates.recv() => {
                    let (pubkey, slot) = res.unwrap();

                },
                _ = refresh_one_interval.tick() => {
                }
            }
        }

        info!("Edge updater job exited..");
        // // send this to unblock the code in front of the exit handler
        // let _ = updater.ready_sender.try_send(());
    });

    listener_job
}