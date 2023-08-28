use mango_feeds_connector::EntityFilter::{FilterByAccountIds, FilterByProgramId};
use mango_feeds_connector::{
    websocket_source, AccountWrite, FilterConfig, GrpcSourceConfig, SlotUpdate,
    SnapshotSourceConfig, SourceConfig,
};

///
/// test with local test-valiator (1.16.1)
///
/// ```
/// RUST_LOG=info solana-test-validator --log
/// solana -ul transfer 2pvrKRRjCtCBUJVZcr6z9QbCPrXLhZRMCpXQYzJuhH9J 0.1
/// ```
///

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    solana_logger::setup_with_default(
        "info,tokio_reactor=info,mango_feeds_connector::websocket_source=debug",
    );

    let config = SourceConfig {
        // only used for geyser
        dedup_queue_size: 50000,
        // only used for geyser
        grpc_sources: vec![],
        // used for websocket+geyser
        snapshot: SnapshotSourceConfig {
            rpc_http_url: "http://127.0.0.1:8899".to_string(),
        },
        // used only for websocket
        rpc_ws_url: "ws://localhost:8900/".to_string(),
    };

    let filter_config1 = FilterConfig {
        entity_filter: FilterByProgramId("11111111111111111111111111111111".to_string()),
    };

    // an account that exists
    let filter_config2 = FilterConfig {
        entity_filter: FilterByAccountIds(vec![
            "2z5cFZAmL5HgDYXPAfEVpWn33Nixsu3iSsg5PDCFDWSb".to_string()
        ]),
    };

    // an account that does not exist
    let filter_config3 = FilterConfig {
        entity_filter: FilterByAccountIds(vec![
            "aorYUvexUBb6cRFpmauF3ofgUDDpFZcRpHpcp5B2Zip".to_string()
        ]),
    };

    let filter_config4 = FilterConfig {
        entity_filter: FilterByAccountIds(vec![]),
    };

    let filter_config = filter_config1;

    let (account_write_queue_sender, account_write_queue_receiver) =
        async_channel::unbounded::<AccountWrite>();

    let (slot_queue_sender, slot_queue_receiver) = async_channel::unbounded::<SlotUpdate>();

    tokio::spawn(async move {
        loop {
            let next = slot_queue_receiver.recv().await.unwrap();
            // println!("got slot: {:?}", next);
        }
    });

    tokio::spawn(async move {
        loop {
            let next = account_write_queue_receiver.recv().await.unwrap();
            println!("got account write: {:?}", next);
        }
    });

    websocket_source::process_events(
        &config,
        &filter_config,
        account_write_queue_sender,
        slot_queue_sender,
    )
    .await;

    Ok(())
}
