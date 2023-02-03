use crate::{
    chain_data::{AccountData, ChainData, SlotData},
    metrics::{MetricType, Metrics},
    AccountWrite, SlotUpdate, serum::SerumEventQueueHeader,
};
use log::*;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use serum_dex::state::EventView;
use solana_sdk::{
    account::{ReadableAccount, WritableAccount},
    clock::Epoch,
    pubkey::Pubkey,
};
use std::{
    borrow::BorrowMut,
    cmp::max,
    collections::{HashMap, HashSet},
};

use crate::metrics::MetricU64;
use anchor_lang::AccountDeserialize;
use mango_v4::state::{
    AnyEvent, EventQueue, EventQueueHeader, EventType, FillEvent, MAX_NUM_EVENTS,
};

#[derive(Clone, Copy, Debug, Serialize)]
pub enum FillUpdateStatus {
    New,
    Revoke,
}

#[derive(Clone, Debug)]
pub struct FillUpdate {
    pub event: FillEvent,
    pub status: FillUpdateStatus,
    pub market: String,
    pub queue: String,
    pub slot: u64,
    pub write_version: u64,
}

#[derive(Clone, Debug)]
pub struct SerumFillUpdate {
    pub event: serum_dex::state::Event,
    pub status: FillUpdateStatus,
    pub market: String,
    pub queue: String,
    pub slot: u64,
    pub write_version: u64,
}

impl Serialize for FillUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let event = base64::encode(bytemuck::bytes_of(&self.event));
        let mut state = serializer.serialize_struct("FillUpdate", 4)?;
        state.serialize_field("event", &event)?;
        state.serialize_field("market", &self.market)?;
        state.serialize_field("queue", &self.queue)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("slot", &self.slot)?;
        state.serialize_field("write_version", &self.write_version)?;

        state.end()
    }
}

impl Serialize for SerumFillUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let event = base64::encode(bytemuck::bytes_of(&self.event));
        let mut state = serializer.serialize_struct("SerumFillUpdate", 4)?;
        state.serialize_field("event", &event)?;
        state.serialize_field("market", &self.market)?;
        state.serialize_field("queue", &self.queue)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("slot", &self.slot)?;
        state.serialize_field("write_version", &self.write_version)?;

        state.end()
    }
}

#[derive(Clone, Debug)]
pub struct FillCheckpoint {
    pub market: String,
    pub queue: String,
    pub events: Vec<FillEvent>,
    pub slot: u64,
    pub write_version: u64,
}

#[derive(Clone, Debug)]
pub struct SerumFillCheckpoint {
    pub market: String,
    pub queue: String,
    pub events: Vec<serum_dex::state::Event>,
    pub slot: u64,
    pub write_version: u64,
}

impl Serialize for FillCheckpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let events: Vec<String> = self
            .events
            .iter()
            .map(|e| base64::encode(bytemuck::bytes_of(e)))
            .collect();
        let mut state = serializer.serialize_struct("FillCheckpoint", 3)?;
        state.serialize_field("events", &events)?;
        state.serialize_field("market", &self.market)?;
        state.serialize_field("queue", &self.queue)?;
        state.serialize_field("slot", &self.slot)?;
        state.serialize_field("write_version", &self.write_version)?;

        state.end()
    }
}

impl Serialize for SerumFillCheckpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let events: Vec<String> = self
            .events
            .iter()
            .map(|e| base64::encode(bytemuck::bytes_of(e)))
            .collect();
        let mut state = serializer.serialize_struct("SerumFillCheckpoint", 3)?;
        state.serialize_field("events", &events)?;
        state.serialize_field("market", &self.market)?;
        state.serialize_field("queue", &self.queue)?;
        state.serialize_field("slot", &self.slot)?;
        state.serialize_field("write_version", &self.write_version)?;

        state.end()
    }
}

pub enum FillEventFilterMessage {
    Update(FillUpdate),
    SerumUpdate(SerumFillUpdate),
    Checkpoint(FillCheckpoint),
    SerumCheckpoint(SerumFillCheckpoint),
}

// couldn't compile the correct struct size / math on m1, fixed sizes resolve this issue
type EventQueueEvents = [AnyEvent; MAX_NUM_EVENTS as usize];

fn publish_changes_perp(
    slot: u64,
    write_version: u64,
    mkt: &(Pubkey, Pubkey),
    header: &EventQueueHeader,
    events: &EventQueueEvents,
    old_seq_num: u64,
    old_events: &EventQueueEvents,
    fill_update_sender: &async_channel::Sender<FillEventFilterMessage>,
    metric_events_new: &mut MetricU64,
    metric_events_change: &mut MetricU64,
    metric_events_drop: &mut MetricU64,
) {
    // seq_num = N means that events (N-QUEUE_LEN) until N-1 are available
    let start_seq_num = max(old_seq_num, header.seq_num)
        .checked_sub(MAX_NUM_EVENTS as u64)
        .unwrap_or(0);
    let mut checkpoint = Vec::new();
    let mkt_pk_string = mkt.0.to_string();
    let evq_pk_string = mkt.1.to_string();
    for seq_num in start_seq_num..header.seq_num {
        let idx = (seq_num % MAX_NUM_EVENTS as u64) as usize;

        // there are three possible cases:
        // 1) the event is past the old seq num, hence guaranteed new event
        // 2) the event is not matching the old event queue
        // 3) all other events are matching the old event queue
        // the order of these checks is important so they are exhaustive
        if seq_num >= old_seq_num {
            debug!(
                "found new event {} idx {} type {}",
                mkt_pk_string, idx, events[idx].event_type as u32
            );

            metric_events_new.increment();

            // new fills are published and recorded in checkpoint
            if events[idx].event_type == EventType::Fill as u8 {
                let fill: FillEvent = bytemuck::cast(events[idx]);
                fill_update_sender
                    .try_send(FillEventFilterMessage::Update(FillUpdate {
                        slot,
                        write_version,
                        event: fill,
                        status: FillUpdateStatus::New,
                        market: mkt_pk_string.clone(),
                        queue: evq_pk_string.clone(),
                    }))
                    .unwrap(); // TODO: use anyhow to bubble up error
                checkpoint.push(fill);
            }
        } else if old_events[idx].event_type != events[idx].event_type
            || old_events[idx].padding != events[idx].padding
        {
            debug!(
                "found changed event {} idx {} seq_num {} header seq num {} old seq num {}",
                mkt_pk_string, idx, seq_num, header.seq_num, old_seq_num
            );

            metric_events_change.increment();

            // first revoke old event if a fill
            if old_events[idx].event_type == EventType::Fill as u8 {
                let fill: FillEvent = bytemuck::cast(old_events[idx]);
                fill_update_sender
                    .try_send(FillEventFilterMessage::Update(FillUpdate {
                        slot,
                        write_version,
                        event: fill,
                        status: FillUpdateStatus::Revoke,
                        market: mkt_pk_string.clone(),
                        queue: evq_pk_string.clone(),
                    }))
                    .unwrap(); // TODO: use anyhow to bubble up error
            }

            // then publish new if its a fill and record in checkpoint
            if events[idx].event_type == EventType::Fill as u8 {
                let fill: FillEvent = bytemuck::cast(events[idx]);
                fill_update_sender
                    .try_send(FillEventFilterMessage::Update(FillUpdate {
                        slot,
                        write_version,
                        event: fill,
                        status: FillUpdateStatus::New,
                        market: mkt_pk_string.clone(),
                        queue: evq_pk_string.clone(),
                    }))
                    .unwrap(); // TODO: use anyhow to bubble up error
                checkpoint.push(fill);
            }
        } else {
            // every already published event is recorded in checkpoint if a fill
            if events[idx].event_type == EventType::Fill as u8 {
                let fill: FillEvent = bytemuck::cast(events[idx]);
                checkpoint.push(fill);
            }
        }
    }

    // in case queue size shrunk due to a fork we need revoke all previous fills
    for seq_num in header.seq_num..old_seq_num {
        let idx = (seq_num % MAX_NUM_EVENTS as u64) as usize;
        debug!(
            "found dropped event {} idx {} seq_num {} header seq num {} old seq num {}",
            mkt_pk_string, idx, seq_num, header.seq_num, old_seq_num
        );

        metric_events_drop.increment();

        if old_events[idx].event_type == EventType::Fill as u8 {
            let fill: FillEvent = bytemuck::cast(old_events[idx]);
            fill_update_sender
                .try_send(FillEventFilterMessage::Update(FillUpdate {
                    slot,
                    event: fill,
                    write_version,
                    status: FillUpdateStatus::Revoke,
                    market: mkt_pk_string.clone(),
                    queue: evq_pk_string.clone(),
                }))
                .unwrap(); // TODO: use anyhow to bubble up error
        }
    }

    fill_update_sender
        .try_send(FillEventFilterMessage::Checkpoint(FillCheckpoint {
            slot,
            write_version,
            events: checkpoint,
            market: mkt_pk_string,
            queue: evq_pk_string,
        }))
        .unwrap()
}

fn publish_changes_serum(
    slot: u64,
    write_version: u64,
    mkt: &(Pubkey, Pubkey),
    header: &SerumEventQueueHeader,
    events: &[serum_dex::state::Event],
    old_seq_num: u64,
    old_events: &[serum_dex::state::Event],
    fill_update_sender: &async_channel::Sender<FillEventFilterMessage>,
    metric_events_new: &mut MetricU64,
    metric_events_change: &mut MetricU64,
    metric_events_drop: &mut MetricU64,
) {
    // seq_num = N means that events (N-QUEUE_LEN) until N-1 are available
    let start_seq_num = max(old_seq_num, header.seq_num)
        .checked_sub(MAX_NUM_EVENTS as u64)
        .unwrap_or(0);
    let mut checkpoint = Vec::new();
    let mkt_pk_string = mkt.0.to_string();
    let evq_pk_string = mkt.1.to_string();
    let header_seq_num = header.seq_num;
    debug!("start seq {} header seq {}", start_seq_num, header_seq_num);
    for seq_num in start_seq_num..header_seq_num {
        let idx = (seq_num % MAX_NUM_EVENTS as u64) as usize;
        let event_view = events[idx].as_view().unwrap();
        let old_event_view = old_events[idx].as_view().unwrap();

        match event_view {
            EventView::Fill { .. } => {
                // there are three possible cases:
                // 1) the event is past the old seq num, hence guaranteed new event
                // 2) the event is not matching the old event queue
                // 3) all other events are matching the old event queue
                // the order of these checks is important so they are exhaustive
                if seq_num >= old_seq_num {
                    debug!("found new serum fill {} idx {}", mkt_pk_string, idx,);

                    metric_events_new.increment();
                    fill_update_sender
                        .try_send(FillEventFilterMessage::SerumUpdate(SerumFillUpdate {
                            slot,
                            write_version,
                            event: events[idx],
                            status: FillUpdateStatus::New,
                            market: mkt_pk_string.clone(),
                            queue: evq_pk_string.clone(),
                        }))
                        .unwrap(); // TODO: use anyhow to bubble up error
                    checkpoint.push(events[idx]);
                    continue;
                }

                // match old_event_view {
                //     EventView::Fill { .. } => {
                //         // every already published event is recorded in checkpoint
                //         checkpoint.push(events[idx]);
                //     }
                //     EventView::Out { .. } => {
                //         debug!(
                //             "found changed event {} idx {} seq_num {} header seq num {} old seq num {}",
                //             mkt_pk_string, idx, seq_num, header_seq_num, old_seq_num
                //         );

                //         metric_events_change.increment();

                //         // first revoke old event
                //         fill_update_sender
                //             .try_send(FillEventFilterMessage::SerumUpdate(SerumFillUpdate {
                //                 slot,
                //                 write_version,
                //                 event: old_events[idx],
                //                 status: FillUpdateStatus::Revoke,
                //                 market: mkt_pk_string.clone(),
                //                 queue: evq_pk_string.clone(),
                //             }))
                //             .unwrap(); // TODO: use anyhow to bubble up error

                //         // then publish new if its a fill and record in checkpoint
                //         fill_update_sender
                //             .try_send(FillEventFilterMessage::SerumUpdate(SerumFillUpdate {
                //                 slot,
                //                 write_version,
                //                 event: events[idx],
                //                 status: FillUpdateStatus::New,
                //                 market: mkt_pk_string.clone(),
                //                 queue: evq_pk_string.clone(),
                //             }))
                //             .unwrap(); // TODO: use anyhow to bubble up error
                //         checkpoint.push(events[idx]);
                //     }
                // }
            }
            _ => continue,
        }
    }

    // // in case queue size shrunk due to a fork we need revoke all previous fills
    // for seq_num in header_seq_num..old_seq_num {
    //     let idx = (seq_num % MAX_NUM_EVENTS as u64) as usize;
    //     let old_event_view = old_events[idx].as_view().unwrap();
    //     debug!(
    //         "found dropped event {} idx {} seq_num {} header seq num {} old seq num {}",
    //         mkt_pk_string, idx, seq_num, header_seq_num, old_seq_num
    //     );

    //     metric_events_drop.increment();

    //     match old_event_view {
    //         EventView::Fill { .. } => {
    //             fill_update_sender
    //                 .try_send(FillEventFilterMessage::SerumUpdate(SerumFillUpdate {
    //                     slot,
    //                     event: old_events[idx],
    //                     write_version,
    //                     status: FillUpdateStatus::Revoke,
    //                     market: mkt_pk_string.clone(),
    //                     queue: evq_pk_string.clone(),
    //                 }))
    //                 .unwrap(); // TODO: use anyhow to bubble up error
    //         }
    //         EventView::Out { .. } => { continue }
    //     }
    // }

    fill_update_sender
        .try_send(FillEventFilterMessage::SerumCheckpoint(
            SerumFillCheckpoint {
                slot,
                write_version,
                events: checkpoint,
                market: mkt_pk_string,
                queue: evq_pk_string,
            },
        ))
        .unwrap()
}

pub async fn init(
    perp_queue_pks: Vec<(Pubkey, Pubkey)>,
    serum_queue_pks: Vec<(Pubkey, Pubkey)>,
    metrics_sender: Metrics,
) -> anyhow::Result<(
    async_channel::Sender<AccountWrite>,
    async_channel::Sender<SlotUpdate>,
    async_channel::Receiver<FillEventFilterMessage>,
)> {
    let metrics_sender = metrics_sender.clone();

    let mut metric_events_new =
        metrics_sender.register_u64("fills_feed_events_new".into(), MetricType::Counter);
    let mut metric_events_new_serum =
        metrics_sender.register_u64("fills_feed_events_new_serum".into(), MetricType::Counter);
    let mut metric_events_change =
        metrics_sender.register_u64("fills_feed_events_change".into(), MetricType::Counter);
    let mut metrics_events_drop =
        metrics_sender.register_u64("fills_feed_events_drop".into(), MetricType::Counter);

    // The actual message may want to also contain a retry count, if it self-reinserts on failure?
    let (account_write_queue_sender, account_write_queue_receiver) =
        async_channel::unbounded::<AccountWrite>();

    // Slot updates flowing from the outside into the single processing thread. From
    // there they'll flow into the postgres sending thread.
    let (slot_queue_sender, slot_queue_receiver) = async_channel::unbounded::<SlotUpdate>();

    // Fill updates can be consumed by client connections, they contain all fills for all markets
    let (fill_update_sender, fill_update_receiver) =
        async_channel::unbounded::<FillEventFilterMessage>();

    let account_write_queue_receiver_c = account_write_queue_receiver.clone();

    let mut chain_cache = ChainData::new();
    let mut perp_events_cache: HashMap<String, EventQueueEvents> = HashMap::new();
    let mut serum_events_cache: HashMap<String, Vec<serum_dex::state::Event>> = HashMap::new();
    let mut seq_num_cache = HashMap::new();
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
                Ok(account_write) = account_write_queue_receiver_c.recv() => {
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
                let mkt_pk = mkt.1;

                match chain_cache.account(&mkt_pk) {
                    Ok(account_info) => {
                        // only process if the account state changed
                        let evq_version = (account_info.slot, account_info.write_version);
                        let evq_pk_string = mkt.1.to_string();
                        trace!("evq {} write_version {:?}", evq_pk_string, evq_version);
                        if evq_version == *last_evq_version {
                            continue;
                        }
                        last_evq_versions.insert(evq_pk_string.clone(), evq_version);

                        let account = &account_info.account;
                        let is_perp = mango_v4::check_id(account.owner());
                        if is_perp {
                            let event_queue =
                                EventQueue::try_deserialize(account.data().borrow_mut()).unwrap();
                            trace!(
                                "evq {} seq_num {}",
                                evq_pk_string,
                                event_queue.header.seq_num
                            );
                            match seq_num_cache.get(&evq_pk_string) {
                                Some(old_seq_num) => match perp_events_cache.get(&evq_pk_string) {
                                    Some(old_events) => publish_changes_perp(
                                        account_info.slot,
                                        account_info.write_version,
                                        mkt,
                                        &event_queue.header,
                                        &event_queue.buf,
                                        *old_seq_num,
                                        old_events,
                                        &fill_update_sender,
                                        &mut metric_events_new,
                                        &mut metric_events_change,
                                        &mut metrics_events_drop,
                                    ),
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
                                    Some(old_events) => publish_changes_serum(
                                        account_info.slot,
                                        account_info.write_version,
                                        mkt,
                                        &header,
                                        &events,
                                        *old_seq_num,
                                        old_events,
                                        &fill_update_sender,
                                        &mut metric_events_new_serum,
                                        &mut metric_events_change,
                                        &mut metrics_events_drop,
                                    ),
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
        fill_update_receiver,
    ))
}
