use bytemuck::cast_ref;
use mango_v4::state::FillEvent;
use serum_dex::{instruction::MarketInstruction, state::EventView};
use solana_geyser_connector_lib::{
    chain_data::{AccountData, ChainData, SlotData},
    metrics::Metrics,
    serum::SerumEventQueueHeader,
    AccountWrite, SlotUpdate,
};

use anchor_lang::AccountDeserialize;
use log::*;
use solana_sdk::{
    account::{ReadableAccount, WritableAccount},
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    stake_history::Epoch,
};
use std::{
    borrow::BorrowMut,
    collections::{BTreeSet, HashMap, HashSet},
    convert::TryFrom,
    str::FromStr,
    time::{Duration, Instant},
};

const MAX_BACKLOG: usize = 2;
const TIMEOUT_INTERVAL: Duration = Duration::from_millis(400);

pub fn init(
    perp_queue_pks: Vec<(Pubkey, Pubkey)>,
    serum_queue_pks: Vec<(Pubkey, Pubkey)>,
    group_pk: Pubkey,
    metrics_sender: Metrics,
) -> anyhow::Result<(
    async_channel::Sender<AccountWrite>,
    async_channel::Sender<SlotUpdate>,
    async_channel::Receiver<Vec<Instruction>>,
)> {
    let metrics_sender = metrics_sender.clone();

    // The actual message may want to also contain a retry count, if it self-reinserts on failure?
    let (account_write_queue_sender, account_write_queue_receiver) =
        async_channel::unbounded::<AccountWrite>();

    // Slot updates flowing from the outside into the single processing thread. From
    // there they'll flow into the postgres sending thread.
    let (slot_queue_sender, slot_queue_receiver) = async_channel::unbounded::<SlotUpdate>();

    // Event queue updates can be consumed by client connections
    let (instruction_sender, instruction_receiver) = async_channel::unbounded::<Vec<Instruction>>();

    let mut chain_cache = ChainData::new(metrics_sender);
    let mut last_cranked_evq_versions = HashMap::<String, (u64, u64, Instant)>::new();

    let all_queue_pks: HashSet<Pubkey> = perp_queue_pks
        .iter()
        .chain(serum_queue_pks.iter())
        .map(|mkt| mkt.1)
        .collect();

    // update handling thread, reads both sloths and account updates
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Ok(account_write) = account_write_queue_receiver.recv() => {
                    if !all_queue_pks.contains(&account_write.pubkey) {
                        continue;
                    }

                    chain_cache.update_account(
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
                    chain_cache.update_slot(SlotData {
                        slot: slot_update.slot,
                        parent: slot_update.parent,
                        status: slot_update.status,
                        chain: 0,
                    });

                }
            }

            for (mkt_pk, evq_pk) in perp_queue_pks.iter() {
                let evq_b58 = evq_pk.to_string();
                let last_evq_version = last_cranked_evq_versions.get(&evq_b58);

                match chain_cache.account(&evq_pk) {
                    Ok(account_info) => {
                        // only process if the account state changed
                        let (is_unchanged, is_on_timeout) = match last_evq_version {
                            Some((slot, write_version, timestamp)) => (
                                account_info.slot == *slot
                                    && account_info.write_version == *write_version,
                                timestamp.elapsed() < TIMEOUT_INTERVAL,
                            ),
                            None => Default::default(),
                        };
                        trace!("mango perp evq={evq_b58} slot={} v={} is_unchanged={is_unchanged} is_on_timeout={is_on_timeout}", account_info.slot, account_info.write_version);
                        if is_unchanged || is_on_timeout {
                            continue;
                        }

                        let account = &account_info.account;
                        let event_queue: mango_v4::state::EventQueue =
                            mango_v4::state::EventQueue::try_deserialize(
                                account.data().borrow_mut(),
                            )
                            .unwrap();

                        // only crank if at least 1 fill or a sufficient events of other categories are buffered
                        let contains_fill_events = event_queue
                            .iter()
                            .find(|e| e.event_type == mango_v4::state::EventType::Fill as u8)
                            .is_some();
                        let has_backlog = event_queue.iter().count() > MAX_BACKLOG;
                        trace!("mango perp evq={evq_b58} slot={} v={} contains_fill_events={contains_fill_events} has_backlog={has_backlog}", account_info.slot, account_info.write_version);
                        if !contains_fill_events && !has_backlog {
                            continue;
                        }

                        let mango_accounts: BTreeSet<_> = event_queue
                            .iter()
                            .take(10)
                            .flat_map(|e| {
                                match mango_v4::state::EventType::try_from(e.event_type)
                                    .expect("mango v4 event")
                                {
                                    mango_v4::state::EventType::Fill => {
                                        let fill: &mango_v4::state::FillEvent = cast_ref(e);
                                        vec![fill.maker, fill.taker]
                                    }
                                    mango_v4::state::EventType::Out => {
                                        let out: &mango_v4::state::OutEvent = cast_ref(e);
                                        vec![out.owner]
                                    }
                                    mango_v4::state::EventType::Liquidate => vec![],
                                }
                            })
                            .collect();

                        let mut ams: Vec<_> = anchor_lang::ToAccountMetas::to_account_metas(
                            &mango_v4::accounts::PerpConsumeEvents {
                                group: group_pk,
                                perp_market: *mkt_pk,
                                event_queue: *evq_pk,
                            },
                            None,
                        );
                        ams.append(
                            &mut mango_accounts
                                .iter()
                                .map(|pk| AccountMeta::new(*pk, false))
                                .collect(),
                        );

                        let ix = Instruction {
                            program_id: mango_v4::id(),
                            accounts: ams,
                            data: anchor_lang::InstructionData::data(
                                &mango_v4::instruction::PerpConsumeEvents { limit: 10 },
                            ),
                        };

                        info!(
                            "mango perp evq={evq_b58} count={} limit=10",
                            event_queue.iter().count()
                        );
                        instruction_sender.send(vec![ix]).await;

                        last_cranked_evq_versions.insert(
                            evq_b58.clone(),
                            (
                                account_info.slot,
                                account_info.write_version,
                                Instant::now(),
                            ),
                        );
                    }
                    Err(_) => info!("chain_cache could not find {evq_b58}"),
                }
            }

            for (mkt_pk, evq_pk) in serum_queue_pks.iter() {
                let evq_b58 = evq_pk.to_string();
                let last_evq_version = last_cranked_evq_versions.get(&evq_b58);

                match chain_cache.account(&evq_pk) {
                    Ok(account_info) => {
                        // only process if the account state changed
                        let (is_unchanged, is_on_timeout) = match last_evq_version {
                            Some((slot, write_version, timestamp)) => (
                                account_info.slot == *slot
                                    && account_info.write_version == *write_version,
                                timestamp.elapsed() < TIMEOUT_INTERVAL,
                            ),
                            None => Default::default(),
                        };
                        trace!("openbook evq={evq_b58} slot={} v={} is_unchanged={is_unchanged} is_on_timeout={is_on_timeout}", account_info.slot, account_info.write_version);
                        if is_unchanged || is_on_timeout {
                            continue;
                        }

                        let account = &account_info.account;

                        let inner_data = &account.data()[5..&account.data().len() - 7];
                        let header_span = std::mem::size_of::<SerumEventQueueHeader>();
                        let header: SerumEventQueueHeader =
                            *bytemuck::from_bytes(&inner_data[..header_span]);
                        let count = header.count;

                        let rest = &inner_data[header_span..];
                        let event_size = std::mem::size_of::<serum_dex::state::Event>();
                        let slop = rest.len() % event_size;
                        let end = rest.len() - slop;
                        let events =
                            bytemuck::cast_slice::<u8, serum_dex::state::Event>(&rest[..end]);
                        let seq_num = header.seq_num;

                        let events: Vec<_> = (0..count)
                            .map(|i| {
                                let offset = (seq_num - count + i) % events.len() as u64;
                                let event: serum_dex::state::Event = events[offset as usize];
                                event.as_view().unwrap()
                            })
                            .collect();

                        // only crank if at least 1 fill or a sufficient events of other categories are buffered
                        let contains_fill_events = events
                            .iter()
                            .find(|e| matches!(e, serum_dex::state::EventView::Fill { .. }))
                            .is_some();
                        let has_backlog = events.len() > MAX_BACKLOG;
                        if !contains_fill_events && !has_backlog {
                            continue;
                        }

                        let oo_pks: BTreeSet<_> = events
                            .iter()
                            .map(|e| match e {
                                EventView::Fill { owner, .. } | EventView::Out { owner, .. } => {
                                    bytemuck::cast_slice::<u64, Pubkey>(owner)[0]
                                }
                            })
                            .collect();

                        let mut ams: Vec<_> = oo_pks
                            .iter()
                            .map(|pk| AccountMeta::new(*pk, false))
                            .collect();

                        // pass two times evq_pk instead of deprecated fee receivers to reduce encoded tx size
                        ams.append(
                            &mut [
                                mkt_pk, evq_pk, evq_pk, /*coin_pk*/
                                evq_pk, /*pc_pk*/
                            ]
                            .iter()
                            .map(|pk| AccountMeta::new(**pk, false))
                            .collect(),
                        );

                        let ix = Instruction {
                            program_id: Pubkey::from_str(
                                "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",
                            )
                            .unwrap(),
                            accounts: ams,
                            data: MarketInstruction::ConsumeEvents(count as u16).pack(),
                        };

                        info!("openbook evq={evq_b58} count={count}");
                        instruction_sender.send(vec![ix]).await;

                        last_cranked_evq_versions.insert(
                            evq_b58.clone(),
                            (
                                account_info.slot,
                                account_info.write_version,
                                Instant::now(),
                            ),
                        );
                    }
                    Err(_) => info!("chain_cache could not find {evq_b58}"),
                }
            }
        }
    });

    Ok((
        account_write_queue_sender,
        slot_queue_sender,
        instruction_receiver,
    ))
}
