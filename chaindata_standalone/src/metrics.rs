use prometheus::{IntCounter, register_int_counter};
lazy_static::lazy_static! {
    pub static ref SNAPSHOT_ACCOUNTS_CNT: IntCounter =
        register_int_counter!("SNAPSHOT_ACCOUNTS_CNT", "Counter").unwrap();
    pub static ref GRPC_PLUMBING_ACCOUNT_MESSAGE_CNT: IntCounter =
        register_int_counter!("GRPC_PLUMBING_ACCOUNT_MESSAGE_CNT", "Counter").unwrap();
    pub static ref GRPC_PLUMBING_SLOT_MESSAGE_CNT: IntCounter =
        register_int_counter!("GRPC_PLUMBING_SLOT_MESSAGE_CNT", "Counter").unwrap();
    pub static ref CHAINDATA_ACCOUNT_WRITE_IN: IntCounter =
        register_int_counter!("CHAINDATA_ACCOUNT_WRITE_IN", "Counter").unwrap();
    pub static ref CHAINDATA_SLOT_UPDATE_IN: IntCounter =
        register_int_counter!("CHAINDATA_SLOT_UPDATE_IN", "Counter").unwrap();
    pub static ref CHAINDATA_UPDATE_ACCOUNT: IntCounter =
        register_int_counter!("CHAINDATA_UPDATE_ACCOUNT", "Counter").unwrap();
    pub static ref CHAINDATA_SNAP_UPDATE_ACCOUNT: IntCounter =
        register_int_counter!("CHAINDATA_SNAP_UPDATE_ACCOUNT", "Counter").unwrap();
    pub static ref ACCOUNT_UPDATE_SENDER: IntCounter =
        register_int_counter!("ACCOUNT_UPDATE_SENDER", "Counter").unwrap();
}