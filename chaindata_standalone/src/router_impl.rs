use log::{info, trace, warn};
use mango_feeds_connector::chain_data::ChainData;
use mango_feeds_connector::{AccountWrite, SlotUpdate};
use solana_sdk::pubkey::Pubkey;
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, error};

pub type ChainDataArcRw = Arc<RwLock<ChainData>>;

// from router project
pub fn start_chaindata_updating(
    chain_data: ChainDataArcRw,
    // = account_write_receiver
    account_writes: async_channel::Receiver<AccountWrite>,
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
                    let Ok(account_write) = res
                    else {
                        warn!("account write channel err {res:?}");
                        continue;
                    };
                    trace!("[account_write_receiver->chain_data] account update for {}@_slot_{} write_version={}",
                        account_write.pubkey, account_write.slot, account_write.write_version);

                    let mut writer = chain_data.write().unwrap();
                    handle_updated_account(&mut writer, account_write, &account_update_sender);

                    let mut batchsize: u32 = 0;
                    let started_at = Instant::now();
                    'batch_loop: while let Ok(res) = account_writes.try_recv() {
                        batchsize += 1;

                        handle_updated_account(&mut writer, res, &account_update_sender);

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
fn handle_updated_account(
    chain_data: &mut RwLockWriteGuard<ChainData>,
    account_write: AccountWrite,
    account_update_sender: &broadcast::Sender<(Pubkey, u64)>,
) {
    use mango_feeds_connector::chain_data::AccountData;
    use solana_sdk::account::WritableAccount;
    use solana_sdk::clock::Epoch;

    trace!("[account_write_receiver->chain_data] .update_account for {}@_slot_{} write_version={}",
        account_write.pubkey, account_write.slot, account_write.write_version);
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

    trace!("[account_write_receiver->account_update_sender] send write for {}@_slot_{} write_version={}",
        account_write.pubkey, account_write.slot, account_write.write_version);

    let _err = account_update_sender.send((account_write.pubkey, account_write.slot));
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
                    trace!("[account_update_sender->...]-> updater.invalidate_one for {}@_slot_{}", pubkey, slot);

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