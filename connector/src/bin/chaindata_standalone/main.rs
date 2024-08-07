mod router_impl;

use std::sync::{Arc, RwLock, RwLockWriteGuard};
use mango_feeds_connector::chain_data::{ChainData, SlotData};

pub type ChainDataArcRw = Arc<RwLock<ChainData>>;

pub fn main() {
    solana_logger::setup_with_default(
        "info,mango_feeds_connector::grpc_plugin_source=debug",
    );

    // let chain_data = Arc::new(RwLock::new(ChainData::new()));
    //     chain_data.clone(),
    //     account_write_receiver,
    //     slot_receiver,
    //     account_update_sender.clone(),
    //     exit_sender.subscribe(),
    // );
    // chain_data.write().unwrap().update_slot(SlotData {
    //     slot: 1,
    //     parent: None,
    //     status: chain_data::SlotStatus::Processed,
    //     chain: 0,
    // });

}

