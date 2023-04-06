use crate::{
    chain_data::{AccountData, ChainData, ChainDataMetrics, SlotData},
    metrics::Metrics,
    AccountWrite, SlotUpdate,
};

use async_trait::async_trait;
use log::*;
use solana_sdk::{account::WritableAccount, pubkey::Pubkey, stake_history::Epoch};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

#[async_trait]
pub trait AccountWriteSink {
    async fn process(&self, pubkey: &Pubkey, account: &AccountData) -> Result<(), String>;
}

#[derive(Clone)]
pub struct AccountWriteRoute {
    pub matched_pubkeys: Vec<Pubkey>,
    pub sink: Arc<dyn AccountWriteSink + Send + Sync>,
    pub timeout_interval: Duration,
}

#[derive(Clone, Debug)]
struct AccountWriteRecord {
    slot: u64,
    write_version: u64,
    timestamp: Instant,
}

pub fn init(
    routes: Vec<AccountWriteRoute>,
    metrics_sender: Metrics,
) -> anyhow::Result<(
    async_channel::Sender<AccountWrite>,
    async_channel::Sender<SlotUpdate>,
)> {
    let (account_write_queue_sender, account_write_queue_receiver) =
        async_channel::unbounded::<AccountWrite>();

    // Slot updates flowing from the outside into this processing thread. From
    // there the AccountWriteRoute::sink() callback is triggered.
    let (slot_queue_sender, slot_queue_receiver) = async_channel::unbounded::<SlotUpdate>();

    let mut chain_data = ChainData::new();
    let mut chain_data_metrics = ChainDataMetrics::new(&metrics_sender);

    let mut last_updated = HashMap::<String, AccountWriteRecord>::new();

    let all_queue_pks: BTreeSet<Pubkey> = routes
        .iter()
        .flat_map(|r| r.matched_pubkeys.iter())
        .map(|pk| pk.clone())
        .collect();

    // update handling thread, reads both slots and account updates
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Ok(account_write) = account_write_queue_receiver.recv() => {
                    if !all_queue_pks.contains(&account_write.pubkey) {
                        trace!("account write skipped {:?}", account_write.pubkey);
                        continue;
                    } else {
                        trace!("account write processed {:?}", account_write.pubkey);
                    }

                    chain_data.update_account(
                        account_write.pubkey,
                        AccountData {
                            slot: account_write.slot,
                            write_version: account_write.write_version,
                            account: WritableAccount::create(
                                account_write.lamports,
                                account_write.data.clone(),
                                account_write.owner,
                                account_write.executable,
                                account_write.rent_epoch as Epoch,
                            ),
                        },
                    );
                }
                Ok(slot_update) = slot_queue_receiver.recv() => {
                    trace!("slot update processed {:?}", slot_update);
                    chain_data.update_slot(SlotData {
                        slot: slot_update.slot,
                        parent: slot_update.parent,
                        status: slot_update.status,
                        chain: 0,
                    });

                }
            }

            chain_data_metrics.report(&chain_data);

            for route in routes.iter() {
                for pk in route.matched_pubkeys.iter() {
                    match chain_data.account(&pk) {
                        Ok(account_info) => {
                            let pk_b58 = pk.to_string();
                            if let Some(record) = last_updated.get(&pk_b58) {
                                let is_unchanged = account_info.slot == record.slot
                                    && account_info.write_version == record.write_version;
                                let is_throttled =
                                    record.timestamp.elapsed() < route.timeout_interval;
                                if is_unchanged || is_throttled {
                                    trace!("skipped is_unchanged={is_unchanged} is_throttled={is_throttled} pk={pk_b58}");
                                    continue;
                                }
                            };

                            match route.sink.process(pk, account_info).await {
                                Ok(()) => {
                                    // todo: metrics
                                    last_updated.insert(
                                        pk_b58.clone(),
                                        AccountWriteRecord {
                                            slot: account_info.slot,
                                            write_version: account_info.write_version,
                                            timestamp: Instant::now(),
                                        },
                                    );
                                }
                                Err(skip_reason) => {
                                    debug!("sink process skipped reason={skip_reason} pk={pk_b58}");
                                    // todo: metrics
                                }
                            }
                        }
                        Err(_) => {
                            debug!("could not find pk in chain data pk={:?}", pk);
                            // todo: metrics
                        }
                    }
                }
            }
        }
    });

    Ok((account_write_queue_sender, slot_queue_sender))
}
