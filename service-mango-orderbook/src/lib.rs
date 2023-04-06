use mango_feeds_lib::OrderbookSide;
use serde::{ser::SerializeStruct, Serialize, Serializer};

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
