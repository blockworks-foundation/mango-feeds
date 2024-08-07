use std::sync::{Arc, RwLock};
use std::thread::sleep;
use log::info;

use tokio::sync::broadcast;
use warp::header::value;

use mango_feeds_connector::chain_data::ChainData;
use mango_feeds_connector::{AccountWrite, SlotUpdate};

mod router_impl;

pub type ChainDataArcRw = Arc<RwLock<ChainData>>;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let (exit_sender, _) = broadcast::channel(1);

    let (account_write_sender, account_write_receiver) = async_channel::unbounded::<AccountWrite>();
    let (slot_sender, slot_receiver) = async_channel::unbounded::<SlotUpdate>();
    let (account_update_sender, _) = broadcast::channel(524288);

    let chain_data = Arc::new(RwLock::new(ChainData::new()));
    router_impl::start_chaindata_updating(
        chain_data.clone(),
        account_write_receiver,
        slot_receiver,
        account_update_sender.clone(),
        exit_sender.subscribe(),
    );

    info!("chaindata standalone started - now wait some time to let it operate ..");
    sleep(std::time::Duration::from_secs(3));
    info!("send exit signal..");
    exit_sender.send(()).unwrap();
    sleep(std::time::Duration::from_secs(1));
    info!("quitting.");
}
