use crate::{
    chain_data::{AccountData, ChainData, SlotData},
    metrics::{MetricType, Metrics},
    orderbook_filter::{base_lots_to_ui_perp, price_lots_to_ui_perp, MarketConfig, OrderbookSide},
    AccountWrite, SlotUpdate,
};
use bytemuck::{cast_slice, Pod, Zeroable};
use chrono::{Utc, TimeZone};
use log::*;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use serum_dex::state::EventView as SpotEvent;
use solana_sdk::{
    account::{ReadableAccount, WritableAccount},
    clock::Epoch,
    pubkey::Pubkey,
};
use std::{
    borrow::BorrowMut,
    cmp::max,
    collections::{HashMap, HashSet},
    convert::identity,
    time::SystemTime,
};

use crate::metrics::MetricU64;
use anchor_lang::AccountDeserialize;
use mango_v4::state::{
    AnyEvent, EventQueue, EventQueueHeader, EventType, FillEvent as PerpFillEvent, Side,
    MAX_NUM_EVENTS,
};

#[derive(Clone, Copy, Debug)]
pub enum FillUpdateStatus {
    New,
    Revoke,
}

impl Serialize for FillUpdateStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            FillUpdateStatus::New => {
                serializer.serialize_unit_variant("FillUpdateStatus", 0, "new")
            }
            FillUpdateStatus::Revoke => {
                serializer.serialize_unit_variant("FillUpdateStatus", 1, "revoke")
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FillEventType {
    Spot,
    Perp,
}

impl Serialize for FillEventType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            FillEventType::Spot => serializer.serialize_unit_variant("FillEventType", 0, "spot"),
            FillEventType::Perp => serializer.serialize_unit_variant("FillEventType", 1, "perp"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FillEvent {
    pub event_type: FillEventType,
    pub maker: bool,
    pub side: OrderbookSide,
    pub timestamp: u64,
    pub seq_num: u64,
    pub owner: String,
    pub order_id: u128,
    pub client_order_id: u64,
    pub fee: f32,
    pub price: f64,
    pub quantity: f64,
}

impl Serialize for FillEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FillEvent", 12)?;
        state.serialize_field("eventType", &self.event_type)?;
        state.serialize_field("maker", &self.maker)?;
        state.serialize_field("side", &self.side)?;
        state.serialize_field("timestamp", &Utc.timestamp_opt(self.timestamp as i64, 0).unwrap().to_rfc3339())?;
        state.serialize_field("seqNum", &self.seq_num)?;
        state.serialize_field("owner", &self.owner)?;
        state.serialize_field("orderId", &self.order_id)?;
        state.serialize_field("clientOrderId", &self.client_order_id)?;
        state.serialize_field("fee", &self.fee)?;
        state.serialize_field("price", &self.price)?;
        state.serialize_field("quantity", &self.quantity)?;
        state.end()
    }
}

impl FillEvent {
    pub fn new_from_perp(event: PerpFillEvent, config: &MarketConfig) -> [Self; 2] {
        let taker_side = match event.taker_side() {
            Side::Ask => OrderbookSide::Ask,
            Side::Bid => OrderbookSide::Bid,
        };
        let maker_side = match event.taker_side() {
            Side::Ask => OrderbookSide::Bid,
            Side::Bid => OrderbookSide::Ask,
        };
        let price = price_lots_to_ui_perp(
            event.price,
            config.base_decimals,
            config.quote_decimals,
            config.base_lot_size,
            config.quote_lot_size,
        );
        let quantity =
            base_lots_to_ui_perp(event.quantity, config.base_decimals, config.quote_decimals);
        [
            FillEvent {
                event_type: FillEventType::Perp,
                maker: true,
                side: maker_side,
                timestamp: event.timestamp,
                seq_num: event.seq_num,
                owner: event.maker.to_string(),
                order_id: event.maker_order_id,
                client_order_id: 0u64,
                fee: event.maker_fee.to_num(),
                price: price,
                quantity: quantity,
            },
            FillEvent {
                event_type: FillEventType::Perp,
                maker: false,
                side: taker_side,
                timestamp: event.timestamp,
                seq_num: event.seq_num,
                owner: event.taker.to_string(),
                order_id: event.taker_order_id,
                client_order_id: event.taker_client_order_id,
                fee: event.taker_fee.to_num(),
                price: price,
                quantity: quantity,
            },
        ]
    }
    pub fn new_from_spot(
        event: SpotEvent,
        timestamp: u64,
        seq_num: u64,
        config: &MarketConfig,
    ) -> Self {
        match event {
            SpotEvent::Fill {
                side,
                maker,
                native_qty_paid,
                native_qty_received,
                native_fee_or_rebate,
                order_id,
                owner,
                client_order_id,
                ..
            } => {
                let side = match side as u8 {
                    0 => OrderbookSide::Bid,
                    1 => OrderbookSide::Ask,
                    _ => panic!("invalid side"),
                };
                let client_order_id: u64 = match client_order_id {
                    Some(id) => id.into(),
                    None => 0u64,
                };

                let base_multiplier = 10u64.pow(config.base_decimals.into()) as u64;
                let quote_multiplier = 10u64.pow(config.quote_decimals.into()) as u64;

                let (price, quantity) = match side {
                    OrderbookSide::Bid => {
                        let price_before_fees = if maker {
                            native_qty_paid + native_fee_or_rebate
                        } else {
                            native_qty_paid - native_fee_or_rebate
                        };
                        
                        let top = price_before_fees * base_multiplier;
                        let bottom = quote_multiplier * native_qty_received;
                        let price = top as f64 / bottom as f64;
                        let quantity = native_qty_received as f64 / base_multiplier as f64;
                        (price, quantity)
                    }
                    OrderbookSide::Ask => {
                        let price_before_fees = if maker {
                            native_qty_received - native_fee_or_rebate
                        } else {
                            native_qty_received + native_fee_or_rebate
                        };

                        let top = price_before_fees * base_multiplier;
                        let bottom = quote_multiplier * native_qty_paid;
                        let price = top as f64 / bottom as f64;
                        let quantity = native_qty_paid as f64 / base_multiplier as f64;
                        (price, quantity)
                    }
                };

                let fee =
                    native_fee_or_rebate as f32 / quote_multiplier as f32;

                FillEvent {
                    event_type: FillEventType::Spot,
                    maker: maker,
                    side,
                    timestamp,
                    seq_num,
                    owner: Pubkey::new(cast_slice(&identity(owner) as &[_])).to_string(),
                    order_id: order_id,
                    client_order_id: client_order_id,
                    fee,
                    price,
                    quantity,
                }
            }
            SpotEvent::Out { .. } => {
                panic!("Can't build FillEvent from SpotEvent::Out")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct FillUpdate {
    pub event: FillEvent,
    pub status: FillUpdateStatus,
    pub market_key: String,
    pub market_name: String,
    pub slot: u64,
    pub write_version: u64,
}

#[derive(Copy, Clone, Debug)]
#[repr(packed)]
pub struct SerumEventQueueHeader {
    _account_flags: u64, // Initialized, EventQueue
    _head: u64,
    count: u64,
    seq_num: u64,
}
unsafe impl Zeroable for SerumEventQueueHeader {}
unsafe impl Pod for SerumEventQueueHeader {}

impl Serialize for FillUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FillUpdate", 6)?;
        state.serialize_field("event", &self.event)?;
        state.serialize_field("marketKey", &self.market_key)?;
        state.serialize_field("marketName", &self.market_name)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("slot", &self.slot)?;
        state.serialize_field("writeVersion", &self.write_version)?;

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
        let mut state = serializer.serialize_struct("FillCheckpoint", 3)?;
        state.serialize_field("events", &self.events)?;
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

fn publish_changes_perp(
    slot: u64,
    write_version: u64,
    mkt: &(Pubkey, MarketConfig),
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
    let evq_pk_string = mkt.1.event_queue.to_string();
    for seq_num in start_seq_num..header.seq_num {
        let idx = (seq_num % MAX_NUM_EVENTS as u64) as usize;

        // there are three possible cases:
        // 1) the event is past the old seq num, hence guaranteed new event
        // 2) the event is not matching the old event queue
        // 3) all other events are matching the old event queue
        // the order of these checks is important so they are exhaustive
        if seq_num >= old_seq_num {
            debug!(
                "found new event {} idx {} type {} slot {} write_version {}",
                mkt_pk_string, idx, events[idx].event_type as u32, slot, write_version
            );

            metric_events_new.increment();

            // new fills are published and recorded in checkpoint
            if events[idx].event_type == EventType::Fill as u8 {
                let fill: PerpFillEvent = bytemuck::cast(events[idx]);
                let fills = FillEvent::new_from_perp(fill, &mkt.1);
                // send event for both maker and taker
                for fill in fills {
                    fill_update_sender
                        .try_send(FillEventFilterMessage::Update(FillUpdate {
                            slot,
                            write_version,
                            event: fill.clone(),
                            status: FillUpdateStatus::New,
                            market_key: mkt_pk_string.clone(),
                            market_name: mkt.1.name.clone(),
                        }))
                        .unwrap(); // TODO: use anyhow to bubble up error
                    checkpoint.push(fill);
                }
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
                let fill: PerpFillEvent = bytemuck::cast(old_events[idx]);
                let fills = FillEvent::new_from_perp(fill, &mkt.1);
                for fill in fills {
                    fill_update_sender
                        .try_send(FillEventFilterMessage::Update(FillUpdate {
                            slot,
                            write_version,
                            event: fill,
                            status: FillUpdateStatus::Revoke,
                            market_key: mkt_pk_string.clone(),
                            market_name: mkt.1.name.clone(),
                        }))
                        .unwrap(); // TODO: use anyhow to bubble up error
                }
            }

            // then publish new if its a fill and record in checkpoint
            if events[idx].event_type == EventType::Fill as u8 {
                let fill: PerpFillEvent = bytemuck::cast(events[idx]);
                let fills = FillEvent::new_from_perp(fill, &mkt.1);
                for fill in fills {
                    fill_update_sender
                        .try_send(FillEventFilterMessage::Update(FillUpdate {
                            slot,
                            write_version,
                            event: fill.clone(),
                            status: FillUpdateStatus::New,
                            market_key: mkt_pk_string.clone(),
                            market_name: mkt.1.name.clone(),
                        }))
                        .unwrap(); // TODO: use anyhow to bubble up error
                    checkpoint.push(fill);
                }
            }
        } else {
            // every already published event is recorded in checkpoint if a fill
            if events[idx].event_type == EventType::Fill as u8 {
                let fill: PerpFillEvent = bytemuck::cast(events[idx]);
                let fills = FillEvent::new_from_perp(fill, &mkt.1);
                for fill in fills {
                    checkpoint.push(fill);
                }
            }
        }
    }

    // in case queue size shrunk due to a fork we need revoke all previous fills
    for seq_num in header.seq_num..old_seq_num {
        let idx = (seq_num % MAX_NUM_EVENTS as u64) as usize;
        debug!(
            "found dropped event {} idx {} seq_num {} header seq num {} old seq num {} slot {} write_version {}",
            mkt_pk_string, idx, seq_num, header.seq_num, old_seq_num, slot, write_version
        );

        metric_events_drop.increment();

        if old_events[idx].event_type == EventType::Fill as u8 {
            let fill: PerpFillEvent = bytemuck::cast(old_events[idx]);
            let fills = FillEvent::new_from_perp(fill, &mkt.1);
            for fill in fills {
                fill_update_sender
                    .try_send(FillEventFilterMessage::Update(FillUpdate {
                        slot,
                        event: fill,
                        write_version,
                        status: FillUpdateStatus::Revoke,
                        market_key: mkt_pk_string.clone(),
                        market_name: mkt.1.name.clone(),
                    }))
                    .unwrap(); // TODO: use anyhow to bubble up error
            }
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
    mkt: &(Pubkey, MarketConfig),
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
    let evq_pk_string = mkt.1.event_queue.to_string();
    let header_seq_num = header.seq_num;
    debug!("start seq {} header seq {}", start_seq_num, header_seq_num);

    // Timestamp for spot events is time scraped
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    for seq_num in start_seq_num..header_seq_num {
        let idx = (seq_num % MAX_NUM_EVENTS as u64) as usize;
        let event_view = events[idx].as_view().unwrap();
        let old_event_view = old_events[idx].as_view().unwrap();

        match event_view {
            SpotEvent::Fill { .. } => {
                // there are three possible cases:
                // 1) the event is past the old seq num, hence guaranteed new event
                // 2) the event is not matching the old event queue
                // 3) all other events are matching the old event queue
                // the order of these checks is important so they are exhaustive
                let fill = FillEvent::new_from_spot(event_view, timestamp, seq_num, &mkt.1);
                if seq_num >= old_seq_num {
                    debug!("found new serum fill {} idx {}", mkt_pk_string, idx,);

                    metric_events_new.increment();
                    fill_update_sender
                        .try_send(FillEventFilterMessage::Update(FillUpdate {
                            slot,
                            write_version,
                            event: fill.clone(),
                            status: FillUpdateStatus::New,
                            market_key: mkt_pk_string.clone(),
                            market_name: mkt.1.name.clone(),
                        }))
                        .unwrap(); // TODO: use anyhow to bubble up error
                    checkpoint.push(fill);
                    continue;
                }

                match old_event_view {
                    SpotEvent::Fill { order_id, .. } => {
                        if order_id != fill.order_id {
                            debug!(
                                "found changed id event {} idx {} seq_num {} header seq num {} old seq num {}",
                                mkt_pk_string, idx, seq_num, header_seq_num, old_seq_num
                            );

                            metric_events_change.increment();

                            let old_fill = FillEvent::new_from_spot(
                                old_event_view,
                                timestamp,
                                seq_num,
                                &mkt.1,
                            );
                            // first revoke old event
                            fill_update_sender
                                .try_send(FillEventFilterMessage::Update(FillUpdate {
                                    slot,
                                    write_version,
                                    event: old_fill,
                                    status: FillUpdateStatus::Revoke,
                                    market_key: mkt_pk_string.clone(),
                                    market_name: mkt.1.name.clone(),
                                }))
                                .unwrap(); // TODO: use anyhow to bubble up error

                            // then publish new
                            fill_update_sender
                                .try_send(FillEventFilterMessage::Update(FillUpdate {
                                    slot,
                                    write_version,
                                    event: fill.clone(),
                                    status: FillUpdateStatus::New,
                                    market_key: mkt_pk_string.clone(),
                                    market_name: mkt.1.name.clone(),
                                }))
                                .unwrap(); // TODO: use anyhow to bubble up error
                        }

                        // record new event in checkpoint
                        checkpoint.push(fill);
                    }
                    SpotEvent::Out { .. } => {
                        debug!(
                            "found changed type event {} idx {} seq_num {} header seq num {} old seq num {}",
                            mkt_pk_string, idx, seq_num, header_seq_num, old_seq_num
                        );

                        metric_events_change.increment();

                        // publish new fill and record in checkpoint
                        fill_update_sender
                            .try_send(FillEventFilterMessage::Update(FillUpdate {
                                slot,
                                write_version,
                                event: fill.clone(),
                                status: FillUpdateStatus::New,
                                market_key: mkt_pk_string.clone(),
                                market_name: mkt.1.name.clone(),
                            }))
                            .unwrap(); // TODO: use anyhow to bubble up error
                        checkpoint.push(fill);
                    }
                }
            }
            _ => continue,
        }
    }

    // in case queue size shrunk due to a fork we need revoke all previous fills
    for seq_num in header_seq_num..old_seq_num {
        let idx = (seq_num % MAX_NUM_EVENTS as u64) as usize;
        let old_event_view = old_events[idx].as_view().unwrap();
        debug!(
            "found dropped event {} idx {} seq_num {} header seq num {} old seq num {}",
            mkt_pk_string, idx, seq_num, header_seq_num, old_seq_num
        );

        metric_events_drop.increment();

        match old_event_view {
            SpotEvent::Fill { .. } => {
                let old_fill = FillEvent::new_from_spot(old_event_view, timestamp, seq_num, &mkt.1);
                fill_update_sender
                    .try_send(FillEventFilterMessage::Update(FillUpdate {
                        slot,
                        event: old_fill,
                        write_version,
                        status: FillUpdateStatus::Revoke,
                        market_key: mkt_pk_string.clone(),
                        market_name: mkt.1.name.clone(),
                    }))
                    .unwrap(); // TODO: use anyhow to bubble up error
            }
            SpotEvent::Out { .. } => continue,
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

pub async fn init(
    perp_market_configs: Vec<(Pubkey, MarketConfig)>,
    spot_market_configs: Vec<(Pubkey, MarketConfig)>,
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
    let mut metric_events_change_serum =
        metrics_sender.register_u64("fills_feed_events_change_serum".into(), MetricType::Counter);
    let mut metrics_events_drop =
        metrics_sender.register_u64("fills_feed_events_drop".into(), MetricType::Counter);
    let mut metrics_events_drop_serum =
        metrics_sender.register_u64("fills_feed_events_drop_serum".into(), MetricType::Counter);

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

    let mut chain_cache = ChainData::new(metrics_sender);
    let mut perp_events_cache: HashMap<String, EventQueueEvents> = HashMap::new();
    let mut serum_events_cache: HashMap<String, Vec<serum_dex::state::Event>> = HashMap::new();
    let mut seq_num_cache = HashMap::new();
    let mut last_evq_versions = HashMap::<String, (u64, u64)>::new();

    let all_market_configs = [perp_market_configs.clone(), spot_market_configs.clone()].concat();
    let perp_queue_pks: Vec<Pubkey> = perp_market_configs
        .iter()
        .map(|x| x.1.event_queue)
        .collect();
    let spot_queue_pks: Vec<Pubkey> = spot_market_configs
        .iter()
        .map(|x| x.1.event_queue)
        .collect();
    let all_queue_pks: HashSet<Pubkey> =
        HashSet::from_iter([perp_queue_pks.clone(), spot_queue_pks.clone()].concat());

    // update handling thread, reads both sloths and account updates
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Ok(account_write) = account_write_queue_receiver_c.recv() => {
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
                Err(e) = slot_queue_receiver.recv() => {
                    warn!("slot update channel err {:?}", e);
                }
                Err(e) = account_write_queue_receiver_c.recv() => {
                    warn!("write update channel err {:?}", e);
                }
            }

            for mkt in all_market_configs.iter() {
                let evq_pk = mkt.1.event_queue;
                let evq_pk_string = evq_pk.to_string();
                let last_evq_version = last_evq_versions
                    .get(&mkt.1.event_queue.to_string())
                    .unwrap_or(&(0, 0));

                match chain_cache.account(&evq_pk) {
                    Ok(account_info) => {
                        // only process if the account state changed
                        let evq_version = (account_info.slot, account_info.write_version);
                        trace!("evq {} write_version {:?}", evq_pk_string, evq_version);
                        if evq_version == *last_evq_version {
                            continue;
                        }
                        if evq_version.0 < last_evq_version.0 {
                            debug!("evq version slot was old");
                            continue;
                        }
                        if evq_version.0 == last_evq_version.0 && evq_version.1 < last_evq_version.1
                        {
                            info!("evq version slot was same and write version was old");
                            continue;
                        }
                        last_evq_versions.insert(evq_pk_string.clone(), evq_version);

                        let account = &account_info.account;
                        let is_perp = mango_v4::check_id(account.owner());
                        if is_perp {
                            let event_queue =
                                EventQueue::try_deserialize(account.data().borrow_mut()).unwrap();
                            info!(
                                "evq {} seq_num {} version {:?}",
                                evq_pk_string, event_queue.header.seq_num, evq_version,
                            );
                            match seq_num_cache.get(&evq_pk_string) {
                                Some(old_seq_num) => match perp_events_cache.get(&evq_pk_string) {
                                    Some(old_events) => publish_changes_perp(
                                        account_info.slot,
                                        account_info.write_version,
                                        &mkt,
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
                                        &mut metric_events_change_serum,
                                        &mut metrics_events_drop_serum,
                                    ),
                                    _ => {
                                        debug!(
                                            "serum_events_cache could not find {}",
                                            evq_pk_string
                                        )
                                    }
                                },
                                _ => debug!("seq_num_cache could not find {}", evq_pk_string),
                            }

                            seq_num_cache.insert(evq_pk_string.clone(), seq_num.clone());
                            serum_events_cache
                                .insert(evq_pk_string.clone(), events.clone().to_vec());
                        }
                    }
                    Err(_) => debug!("chain_cache could not find {}", mkt.1.event_queue),
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
