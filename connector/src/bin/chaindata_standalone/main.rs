use std::sync::{Arc, RwLock};
use std::thread::sleep;

use tokio::sync::broadcast;

use mango_feeds_connector::{AccountWrite, SlotUpdate};
use mango_feeds_connector::chain_data::ChainData;

mod router_impl;

pub type ChainDataArcRw = Arc<RwLock<ChainData>>;

pub fn main() {
    solana_logger::setup_with_default(
        "info,mango_feeds_connector::grpc_plugin_source=debug",
    );

    let (exit_sender, _) = broadcast::channel(1);

    let (account_write_sender, account_write_receiver) = async_channel::unbounded::<AccountWrite>();
    let (slot_sender, slot_receiver) = async_channel::unbounded::<SlotUpdate>();
    let (account_update_sender, _) = broadcast::channel(524288); // TODO this is huge, nut init snapshot will completely spam this


    let chain_data = Arc::new(RwLock::new(ChainData::new()));
    router_impl::start_chaindata_updating(
        chain_data.clone(),
        account_write_receiver,
        slot_receiver,
        account_update_sender.clone(),
        exit_sender.subscribe(),
    );


    sleep(std::time::Duration::from_secs(10));

}

