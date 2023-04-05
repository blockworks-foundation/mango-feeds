use crate::metrics::MetricU64;
use crate::{
    chain_data::{AccountData, ChainData, ChainDataMetrics, SlotData},
    metrics::{MetricType, Metrics},
    AccountWrite, SlotUpdate,
};
use anchor_lang::AccountDeserialize;
use itertools::Itertools;
use log::*;
use mango_v4::{
    serum3_cpi::OrderBookStateHeader,
    state::{BookSide, OrderTreeType},
};
use serde::{ser::SerializeStruct, Serialize, Serializer};
use serum_dex::critbit::Slab;
use solana_sdk::{
    account::{ReadableAccount, WritableAccount},
    clock::Epoch,
    pubkey::Pubkey,
};
use std::{
    borrow::BorrowMut,
    collections::{HashMap, HashSet},
    mem::size_of,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug)]
pub enum OrderbookSide {
    Bid = 0,
    Ask = 1,
}

impl Serialize for OrderbookSide {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            OrderbookSide::Bid => serializer.serialize_unit_variant("Side", 0, "bid"),
            OrderbookSide::Ask => serializer.serialize_unit_variant("Side", 1, "ask"),
        }
    }
}

pub type OrderbookLevel = [f64; 2];

#[derive(Clone, Debug)]
pub struct OrderbookUpdate {
    pub market: String,
    pub side: OrderbookSide,
    pub update: Vec<OrderbookLevel>,
    pub slot: u64,
    pub write_version: u64,
}

impl Serialize for OrderbookUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("OrderbookUpdate", 5)?;
        state.serialize_field("market", &self.market)?;
        state.serialize_field("side", &self.side)?;
        state.serialize_field("update", &self.update)?;
        state.serialize_field("slot", &self.slot)?;
        state.serialize_field("write_version", &self.write_version)?;

        state.end()
    }
}

#[derive(Clone, Debug)]
pub struct OrderbookCheckpoint {
    pub market: String,
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
    pub slot: u64,
    pub write_version: u64,
}

impl Serialize for OrderbookCheckpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("OrderbookCheckpoint", 3)?;
        state.serialize_field("market", &self.market)?;
        state.serialize_field("bids", &self.bids)?;
        state.serialize_field("asks", &self.asks)?;
        state.serialize_field("slot", &self.slot)?;
        state.serialize_field("write_version", &self.write_version)?;

        state.end()
    }
}

pub enum OrderbookFilterMessage {
    Update(OrderbookUpdate),
    Checkpoint(OrderbookCheckpoint),
}

#[derive(Clone, Debug)]
pub struct MarketConfig {
    pub name: String,
    pub bids: Pubkey,
    pub asks: Pubkey,
    pub event_queue: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub base_lot_size: i64,
    pub quote_lot_size: i64,
}

pub fn base_lots_to_ui(native: i64, base_decimals: u8, base_lot_size: i64) -> f64 {
    (native * base_lot_size) as f64 / 10i64.pow(base_decimals.into()) as f64
}

pub fn base_lots_to_ui_perp(native: i64, decimals: u8, base_lot_size: i64) -> f64 {
    native as f64 * (base_lot_size as f64 / (10i64.pow(decimals.into()) as f64))
}

pub fn price_lots_to_ui(native: i64, base_decimals: u8, quote_decimals: u8) -> f64 {
    let decimals = base_decimals - quote_decimals;
    native as f64 / (10u64.pow(decimals.into())) as f64
}

pub fn spot_price_to_ui(
    native: i64,
    native_size: i64,
    base_decimals: u8,
    quote_decimals: u8,
) -> f64 {
    // TODO: account for fees
    ((native * 10i64.pow(base_decimals.into())) / (10i64.pow(quote_decimals.into()) * native_size))
        as f64
}

pub fn price_lots_to_ui_perp(
    native: i64,
    base_decimals: u8,
    quote_decimals: u8,
    base_lot_size: i64,
    quote_lot_size: i64,
) -> f64 {
    let decimals = base_decimals - quote_decimals;
    let multiplier = 10u64.pow(decimals.into()) as f64;
    native as f64 * ((multiplier * quote_lot_size as f64) / base_lot_size as f64)
}

fn publish_changes(
    slot: u64,
    write_version: u64,
    mkt: &(Pubkey, MarketConfig),
    side: OrderbookSide,
    current_bookside: &Vec<OrderbookLevel>,
    previous_bookside: &Vec<OrderbookLevel>,
    maybe_other_bookside: Option<&Vec<OrderbookLevel>>,
    orderbook_update_sender: &async_channel::Sender<OrderbookFilterMessage>,
    metric_updates: &mut MetricU64,
) {
    let mut update: Vec<OrderbookLevel> = vec![];
    // push diff for levels that are no longer present
    if current_bookside.len() != previous_bookside.len() {
        info!(
            "L {}",
            current_bookside.len() as i64 - previous_bookside.len() as i64
        )
    }

    for previous_order in previous_bookside.iter() {
        let peer = current_bookside
            .iter()
            .find(|level| previous_order[0] == level[0]);

        match peer {
            None => {
                info!("R {} {}", previous_order[0], previous_order[1]);
                update.push([previous_order[0], 0f64]);
            }
            _ => continue,
        }
    }

    // push diff where there's a new level or size has changed
    for current_order in current_bookside {
        let peer = previous_bookside
            .iter()
            .find(|item| item[0] == current_order[0]);

        match peer {
            Some(previous_order) => {
                if previous_order[1] == current_order[1] {
                    continue;
                }
                info!(
                    "C {} {} -> {}",
                    current_order[0], previous_order[1], current_order[1]
                );
                update.push(current_order.clone());
            }
            None => {
                info!("A {} {}", current_order[0], current_order[1]);
                update.push(current_order.clone())
            }
        }
    }

    match maybe_other_bookside {
        Some(other_bookside) => {
            let (bids, asks) = match side {
                OrderbookSide::Bid => (current_bookside, other_bookside),
                OrderbookSide::Ask => (other_bookside, current_bookside),
            };
            orderbook_update_sender
                .try_send(OrderbookFilterMessage::Checkpoint(OrderbookCheckpoint {
                    slot,
                    write_version,
                    bids: bids.clone(),
                    asks: asks.clone(),
                    market: mkt.0.to_string(),
                }))
                .unwrap()
        }
        None => info!("other bookside not in cache"),
    }

    if update.len() == 0 {
        return;
    }

    orderbook_update_sender
        .try_send(OrderbookFilterMessage::Update(OrderbookUpdate {
            market: mkt.0.to_string(),
            side: side.clone(),
            update,
            slot,
            write_version,
        }))
        .unwrap(); // TODO: use anyhow to bubble up error
    metric_updates.increment();
}

pub async fn init(
    market_configs: Vec<(Pubkey, MarketConfig)>,
    serum_market_configs: Vec<(Pubkey, MarketConfig)>,
    metrics_sender: Metrics,
) -> anyhow::Result<(
    async_channel::Sender<AccountWrite>,
    async_channel::Sender<SlotUpdate>,
    async_channel::Receiver<OrderbookFilterMessage>,
)> {
    let metrics_sender = metrics_sender.clone();

    let mut metric_events_new =
        metrics_sender.register_u64("orderbook_updates".into(), MetricType::Counter);

    // The actual message may want to also contain a retry count, if it self-reinserts on failure?
    let (account_write_queue_sender, account_write_queue_receiver) =
        async_channel::unbounded::<AccountWrite>();

    // Slot updates flowing from the outside into the single processing thread. From
    // there they'll flow into the postgres sending thread.
    let (slot_queue_sender, slot_queue_receiver) = async_channel::unbounded::<SlotUpdate>();

    // Fill updates can be consumed by client connections, they contain all fills for all markets
    let (fill_update_sender, fill_update_receiver) =
        async_channel::unbounded::<OrderbookFilterMessage>();

    let account_write_queue_receiver_c = account_write_queue_receiver.clone();

    let mut chain_cache = ChainData::new();
    let mut chain_data_metrics = ChainDataMetrics::new(&metrics_sender);
    let mut bookside_cache: HashMap<String, Vec<OrderbookLevel>> = HashMap::new();
    let mut serum_bookside_cache: HashMap<String, Vec<OrderbookLevel>> = HashMap::new();
    let mut last_write_versions = HashMap::<String, (u64, u64)>::new();

    let relevant_pubkeys = [market_configs.clone(), serum_market_configs.clone()]
        .concat()
        .iter()
        .flat_map(|m| [m.1.bids, m.1.asks])
        .collect::<HashSet<Pubkey>>();
    info!("relevant_pubkeys {:?}", relevant_pubkeys);
    // update handling thread, reads both slots and account updates
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

            chain_data_metrics.report(&chain_cache);

            for mkt in market_configs.iter() {
                for side in 0..2 {
                    let mkt_pk = mkt.0;
                    let side_pk = if side == 0 { mkt.1.bids } else { mkt.1.asks };
                    let other_side_pk = if side == 0 { mkt.1.asks } else { mkt.1.bids };
                    let last_write_version = last_write_versions
                        .get(&side_pk.to_string())
                        .unwrap_or(&(0, 0));

                    match chain_cache.account(&side_pk) {
                        Ok(account_info) => {
                            let side_pk_string = side_pk.to_string();

                            let write_version = (account_info.slot, account_info.write_version);
                            if write_version == *last_write_version {
                                continue;
                            }
                            if write_version.0 < last_write_version.0 {
                                debug!("evq version slot was old");
                                continue;
                            }
                            if write_version.0 == last_write_version.0
                                && write_version.1 < last_write_version.1
                            {
                                debug!("evq version slot was same and write version was old");
                                continue;
                            }
                            last_write_versions.insert(side_pk_string.clone(), write_version);

                            let account = &account_info.account;
                            let bookside =
                                BookSide::try_deserialize(account.data().borrow_mut()).unwrap();
                            let side = match bookside.nodes.order_tree_type() {
                                OrderTreeType::Bids => OrderbookSide::Bid,
                                OrderTreeType::Asks => OrderbookSide::Ask,
                            };
                            let time_now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                            let oracle_price_lots = 0; // todo: does this matter? where to find it?
                            let bookside = bookside
                                .iter_valid(time_now, oracle_price_lots)
                                .map(|item| (item.node.price_data() as i64, item.node.quantity))
                                .group_by(|(price, _)| *price)
                                .into_iter()
                                .map(|(price, group)| {
                                    [
                                        price_lots_to_ui_perp(
                                            price,
                                            mkt.1.base_decimals,
                                            mkt.1.quote_decimals,
                                            mkt.1.base_lot_size,
                                            mkt.1.quote_lot_size,
                                        ),
                                        base_lots_to_ui_perp(
                                            group
                                                .map(|(_, quantity)| quantity)
                                                .fold(0, |acc, x| acc + x),
                                            mkt.1.base_decimals,
                                            mkt.1.base_lot_size,
                                        ),
                                    ]
                                })
                                .collect();

                            let other_bookside = bookside_cache.get(&other_side_pk.to_string());

                            match bookside_cache.get(&side_pk_string) {
                                Some(old_bookside) => publish_changes(
                                    account_info.slot,
                                    account_info.write_version,
                                    mkt,
                                    side,
                                    &bookside,
                                    &old_bookside,
                                    other_bookside,
                                    &fill_update_sender,
                                    &mut metric_events_new,
                                ),
                                _ => info!("bookside_cache could not find {}", side_pk_string),
                            }

                            bookside_cache.insert(side_pk_string.clone(), bookside.clone());
                        }
                        Err(_) => debug!("chain_cache could not find {}", mkt_pk),
                    }
                }
            }

            for mkt in serum_market_configs.iter() {
                for side in 0..2 {
                    let side_pk = if side == 0 { mkt.1.bids } else { mkt.1.asks };
                    let other_side_pk = if side == 0 { mkt.1.asks } else { mkt.1.bids };
                    let last_write_version = last_write_versions
                        .get(&side_pk.to_string())
                        .unwrap_or(&(0, 0));

                    match chain_cache.account(&side_pk) {
                        Ok(account_info) => {
                            let side_pk_string = side_pk.to_string();

                            let write_version = (account_info.slot, account_info.write_version);
                            // todo: should this be <= so we don't overwrite with old data received late?
                            if write_version <= *last_write_version {
                                continue;
                            }
                            last_write_versions.insert(side_pk_string.clone(), write_version);
                            debug!("W {}", mkt.1.name);
                            let account = &mut account_info.account.clone();
                            let data = account.data_as_mut_slice();
                            let len = data.len();
                            let inner = &mut data[5..len - 7];
                            let slab = Slab::new(&mut inner[size_of::<OrderBookStateHeader>()..]);

                            let bookside: Vec<OrderbookLevel> = slab
                                .iter(side == 0)
                                .map(|item| {
                                    (u64::from(item.price()) as i64, item.quantity() as i64)
                                })
                                .group_by(|(price, _)| *price)
                                .into_iter()
                                .map(|(price, group)| {
                                    [
                                        price_lots_to_ui(
                                            price,
                                            mkt.1.base_decimals,
                                            mkt.1.quote_decimals,
                                        ),
                                        base_lots_to_ui(
                                            group
                                                .map(|(_, quantity)| quantity)
                                                .fold(0, |acc, x| acc + x),
                                            mkt.1.base_decimals,
                                            mkt.1.base_lot_size,
                                        ),
                                    ]
                                })
                                .collect();

                            let other_bookside =
                                serum_bookside_cache.get(&other_side_pk.to_string());

                            match serum_bookside_cache.get(&side_pk_string) {
                                Some(old_bookside) => publish_changes(
                                    account_info.slot,
                                    account_info.write_version,
                                    mkt,
                                    if side == 0 {
                                        OrderbookSide::Bid
                                    } else {
                                        OrderbookSide::Ask
                                    },
                                    &bookside,
                                    old_bookside,
                                    other_bookside,
                                    &fill_update_sender,
                                    &mut metric_events_new,
                                ),
                                _ => info!("bookside_cache could not find {}", side_pk_string),
                            }

                            serum_bookside_cache.insert(side_pk_string.clone(), bookside);
                        }
                        Err(_) => debug!("chain_cache could not find {}", side_pk),
                    }
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
