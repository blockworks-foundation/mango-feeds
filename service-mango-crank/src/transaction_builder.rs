use crate::Pubkey;
use bytemuck::cast_ref;
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
    instruction::{Instruction, AccountMeta},
    stake_history::Epoch,
};
use std::{
    borrow::BorrowMut,
    collections::{HashMap, HashSet}, iter::{once, empty},
    convert::TryFrom
};
 
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
    let mut perp_events_cache = HashMap::<
        String,
        [mango_v4::state::AnyEvent; mango_v4::state::MAX_NUM_EVENTS as usize],
    >::new();
    let mut serum_events_cache = HashMap::<String, Vec<serum_dex::state::Event>>::new();
    let mut seq_num_cache = HashMap::<String, u64>::new();
    let mut last_evq_versions = HashMap::<String, (u64, u64)>::new();

    let all_queue_pks = [perp_queue_pks.clone(), serum_queue_pks.clone()].concat();
    let relevant_pubkeys = all_queue_pks
        .iter()
        .map(|m| m.1)
        .collect::<HashSet<Pubkey>>();

    // update handling thread, reads both sloths and account updates
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Ok(account_write) = account_write_queue_receiver.recv() => {
                    if !relevant_pubkeys.contains(&account_write.pubkey) {
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

            for mkt in all_queue_pks.iter() {
                let last_evq_version = last_evq_versions.get(&mkt.1.to_string()).unwrap_or(&(0, 0));
                let mkt_pk = mkt.0;
                let evq_pk = mkt.1;

                match chain_cache.account(&evq_pk) {
                    Ok(account_info) => {
                        // only process if the account state changed
                        let evq_version = (account_info.slot, account_info.write_version);
                        let evq_pk_string = evq_pk.to_string();
                        trace!("evq {} write_version {:?}", evq_pk_string, evq_version);
                        if evq_version == *last_evq_version {
                            continue;
                        }
                        last_evq_versions.insert(evq_pk_string.clone(), evq_version);

                        let account = &account_info.account;
                        let is_perp = mango_v4::check_id(account.owner());
                        if is_perp {
                            let event_queue: mango_v4::state::EventQueue = mango_v4::state::EventQueue::try_deserialize(
                                account.data().borrow_mut(),
                            )
                            .unwrap();
                            trace!(
                                "evq {} seq_num {}",
                                evq_pk_string,
                                event_queue.header.seq_num
                            );

                            if !event_queue.empty() {
                                let mango_accounts: HashSet<_> = event_queue.iter().take(10).flat_map(|e|  match mango_v4::state::EventType::try_from(e.event_type).expect("mango v4 event") {
                                    mango_v4::state::EventType::Fill => {
                                        let fill: &mango_v4::state::FillEvent = cast_ref(e);
                                        vec![fill.maker, fill.taker]
                                    }
                                    mango_v4::state::EventType::Out => {
                                        let out: &mango_v4::state::OutEvent = cast_ref(e);
                                        vec![out.owner]
                                    }
                                    mango_v4::state::EventType::Liquidate => vec![]
                                })
                                .collect();

                                let mut ams: Vec<_> = anchor_lang::ToAccountMetas::to_account_metas(
                                    &mango_v4::accounts::PerpConsumeEvents {
                                        group: group_pk,
                                        perp_market: mkt_pk,
                                        event_queue: evq_pk,
                                    },
                                    None,
                                );

                                ams.append(&mut mango_accounts
                                    .iter()
                                    .map(|pk| AccountMeta { pubkey: *pk, is_signer: false, is_writable: true })
                                    .collect());

                                let ix = Instruction {
                                    program_id: mango_v4::id(),
                                    accounts: ams,
                                    data: anchor_lang::InstructionData::data(&mango_v4::instruction::PerpConsumeEvents {
                                        limit: 10,
                                    }),
                                };

                                instruction_sender.send(vec![ix]).await;
                            }


                            
                            match seq_num_cache.get(&evq_pk_string) {
                                Some(old_seq_num) => match perp_events_cache.get(&evq_pk_string) {
                                    Some(old_events) => {}
                                    _ => {
                                        info!("perp_events_cache could not find {}", evq_pk_string)
                                    }
                                },
                                _ => info!("seq_num_cache could not find {}", evq_pk_string),
                            }

                            seq_num_cache
                                .insert(evq_pk_string.clone(), event_queue.header.seq_num.clone());
                            perp_events_cache
                                .insert(evq_pk_string.clone(), event_queue.buf.clone());
                        } else {
                            let inner_data = &account.data()[5..&account.data().len() - 7];
                            let header_span = std::mem::size_of::<SerumEventQueueHeader>();
                            let header: SerumEventQueueHeader =
                                *bytemuck::from_bytes(&inner_data[..header_span]);
                            let seq_num = header.seq_num;
                            let count = header.count;
                            let rest = &inner_data[header_span..];
                            let slop = rest.len() % std::mem::size_of::<serum_dex::state::Event>();
                            let new_len = rest.len() - slop;
                            let events = &rest[..new_len];
                            debug!("evq {} header_span {} header_seq_num {} header_count {} inner_len {} events_len {} sizeof Event {}", evq_pk_string, header_span, seq_num, count, inner_data.len(), events.len(), std::mem::size_of::<serum_dex::state::Event>());
                            let events: &[serum_dex::state::Event] = bytemuck::cast_slice(&events);

                            match seq_num_cache.get(&evq_pk_string) {
                                Some(old_seq_num) => match serum_events_cache.get(&evq_pk_string) {
                                    Some(old_events) => {}
                                    _ => {
                                        info!("serum_events_cache could not find {}", evq_pk_string)
                                    }
                                },
                                _ => info!("seq_num_cache could not find {}", evq_pk_string),
                            }

                            seq_num_cache.insert(evq_pk_string.clone(), seq_num.clone());
                            serum_events_cache
                                .insert(evq_pk_string.clone(), events.clone().to_vec());
                        }
                    }
                    Err(_) => info!("chain_cache could not find {}", mkt.1),
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
