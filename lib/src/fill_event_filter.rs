use crate::{
    chain_data::{AccountData, ChainData, SlotData},
    metrics::{MetricType, Metrics},
    AccountWrite, SlotUpdate,
};
use log::*;
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};
use solana_sdk::{
    account::{ReadableAccount, WritableAccount},
    clock::Epoch,
    pubkey::Pubkey,
};
use std::{
    borrow::BorrowMut,
    cmp::max,
    collections::{HashMap, HashSet},
    str::FromStr,
};

use crate::metrics::MetricU64;
use anchor_lang::AccountDeserialize;
use mango_v4::state::{
    AnyEvent, EventQueue, EventQueueHeader, EventType, FillEvent, MAX_NUM_EVENTS,
};

#[derive(Clone, Debug, Deserialize)]
pub struct MarketConfig {
    pub name: String,
    pub event_queue: String,
}

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

impl Serialize for FillUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let event = base64::encode_config(bytemuck::bytes_of(&self.event), base64::STANDARD);
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

#[derive(Clone, Debug)]
pub struct FillCheckpoint {
    pub market: String,
    pub queue: String,
    pub events: Vec<FillEvent>,
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
            .map(|e| base64::encode_config(bytemuck::bytes_of(e), base64::STANDARD))
            .collect();
        let mut state = serializer.serialize_struct("FillUpdate", 3)?;
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
    Checkpoint(FillCheckpoint),
}

// couldn't compile the correct struct size / math on m1, fixed sizes resolve this issue
type EventQueueEvents = [AnyEvent; MAX_NUM_EVENTS as usize];

fn publish_changes(
    slot: u64,
    write_version: u64,
    mkt: &MarketConfig,
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
                mkt.name, idx, events[idx].event_type as u32
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
                        market: mkt.name.clone(),
                        queue: mkt.event_queue.clone(),
                    }))
                    .unwrap(); // TODO: use anyhow to bubble up error
                checkpoint.push(fill);
            }
        } else if old_events[idx].event_type != events[idx].event_type
            || old_events[idx].padding != events[idx].padding
        {
            debug!(
                "found changed event {} idx {} seq_num {} header seq num {} old seq num {}",
                mkt.name, idx, seq_num, header.seq_num, old_seq_num
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
                        market: mkt.name.clone(),
                        queue: mkt.event_queue.clone(),
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
                        market: mkt.name.clone(),
                        queue: mkt.event_queue.clone(),
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
            mkt.name, idx, seq_num, header.seq_num, old_seq_num
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
                    market: mkt.name.clone(),
                    queue: mkt.event_queue.clone(),
                }))
                .unwrap(); // TODO: use anyhow to bubble up error
        }
    }

    fill_update_sender
        .try_send(FillEventFilterMessage::Checkpoint(FillCheckpoint {
            slot,
            write_version,
            events: checkpoint,
            market: mkt.name.clone(),
            queue: mkt.event_queue.clone(),
        }))
        .unwrap()
}

pub async fn init(
    markets: Vec<MarketConfig>,
    metrics_sender: Metrics,
) -> anyhow::Result<(
    async_channel::Sender<AccountWrite>,
    async_channel::Sender<SlotUpdate>,
    async_channel::Receiver<FillEventFilterMessage>,
)> {
    let metrics_sender = metrics_sender.clone();

    let mut metric_events_new =
        metrics_sender.register_u64("fills_feed_events_new".into(), MetricType::Counter);
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
    let mut events_cache: HashMap<String, EventQueueEvents> = HashMap::new();
    let mut seq_num_cache = HashMap::new();
    let mut last_ev_q_versions = HashMap::<String, (u64, u64)>::new();

    let relevant_pubkeys = markets
        .iter()
        .map(|m| Pubkey::from_str(&m.event_queue).unwrap())
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

            for mkt in markets.iter() {
                let last_ev_q_version = last_ev_q_versions.get(&mkt.event_queue).unwrap_or(&(0, 0));
                let mkt_pk = mkt.event_queue.parse::<Pubkey>().unwrap();

                match chain_cache.account(&mkt_pk) {
                    Ok(account_info) => {
                        // only process if the account state changed
                        let ev_q_version = (account_info.slot, account_info.write_version);
                        trace!("evq {} write_version {:?}", mkt.name, ev_q_version);
                        if ev_q_version == *last_ev_q_version {
                            continue;
                        }
                        last_ev_q_versions.insert(mkt.event_queue.clone(), ev_q_version);

                        let account = &account_info.account;

                        let event_queue =
                            EventQueue::try_deserialize(account.data().borrow_mut()).unwrap();
                        trace!("evq {} seq_num {}", mkt.name, event_queue.header.seq_num);

                        match seq_num_cache.get(&mkt.event_queue) {
                            Some(old_seq_num) => match events_cache.get(&mkt.event_queue) {
                                Some(old_events) => publish_changes(
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
                                _ => info!("events_cache could not find {}", mkt.name),
                            },
                            _ => info!("seq_num_cache could not find {}", mkt.name),
                        }

                        seq_num_cache
                            .insert(mkt.event_queue.clone(), event_queue.header.seq_num.clone());
                        events_cache.insert(mkt.event_queue.clone(), event_queue.buf.clone());
                    }
                    Err(_) => info!("chain_cache could not find {}", mkt.name),
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
