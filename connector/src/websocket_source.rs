use futures::stream::{SelectAll, StreamExt};
use jsonrpc_core_client::transports::ws;

use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_client::{
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_response::{Response, RpcKeyedAccount},
};
use solana_rpc::rpc_pubsub::RpcSolPubSubClient;
use solana_sdk::{
    account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey, slot_history::Slot,
};

use log::*;
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use std::ops::Sub;
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::Context;
use solana_client::rpc_response::RpcBlockUpdateError::UnsupportedTransactionVersion;
use tokio::time::timeout;

use crate::{chain_data::SlotStatus, AccountWrite, AnyhowWrap, FilterConfig, SlotUpdate, SourceConfig, EntityFilter};
use crate::debouncer::Debouncer;
use crate::snapshot::{get_snapshot_gma, get_snapshot_gpa, SnapshotMultipleAccounts, SnapshotProgramAccounts};

const SNAPSHOT_MAX_AGE: Duration = Duration::from_secs(300);
const WS_CONNECT_TIMEOUT: Duration = Duration::from_millis(5000);


enum WebsocketMessage {
    SingleUpdate(Response<RpcKeyedAccount>),
    SnapshotUpdate((Slot, Vec<(String, Option<UiAccount>)>)),
    SlotUpdate(Arc<solana_client::rpc_response::SlotUpdate>),
}

async fn feed_data(
    config: &SourceConfig,
    filter_config: &FilterConfig,
    sender: async_channel::Sender<WebsocketMessage>,
    debouncer_errorlog: Arc<Debouncer>,
) -> anyhow::Result<()> {

    match &filter_config.entity_filter {
        EntityFilter::FilterByAccountIds(account_ids) => feed_data_by_account(config, account_ids.clone(), sender, debouncer_errorlog).await,
        EntityFilter::FilterByProgramId(program_id) => feed_data_by_program(config, program_id.clone(), sender, debouncer_errorlog).await,
    }
}

// TODO: the reconnecting should be part of this
// consume data until an error happens; error must be handled by caller by reconnecting
async fn feed_data_by_account(
    config: &SourceConfig,
    account_ids: Vec<String>,
    sender: async_channel::Sender<WebsocketMessage>,
    debouncer_errorlog: Arc<Debouncer>,
) -> anyhow::Result<()> {

    let rpc_ws_url: &str = &config.rpc_ws_url;

    let connect = ws::try_connect::<RpcSolPubSubClient>(rpc_ws_url).map_err_anyhow()?;
    let timeout = timeout(WS_CONNECT_TIMEOUT, connect).await?;
    let client = timeout.map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::processed()),
        data_slice: None,
        min_context_slot: None,
    };
    let program_accounts_config = RpcProgramAccountsConfig {
        filters: None,
        with_context: Some(true),
        account_config: account_info_config.clone(),
    };

    let mut account_subs = SelectAll::new();
    // let mut program_subs = SelectAll::new();

    for account_id in &account_ids {
        account_subs.push(
            client
                .account_subscribe(account_id.clone(), Some(account_info_config.clone()))
                .map(|s| {
                    let account_id = account_id.clone();
                    s.map(move |r| (account_id.clone(), r))
                })
                .map_err_anyhow()?,
        );
    }

    let mut slot_sub = client.slots_updates_subscribe().map_err_anyhow()?;

    let mut last_snapshot = Instant::now().sub(SNAPSHOT_MAX_AGE);

    // consume from channels unitl an error happens
    loop {

        // occasionally cause a new snapshot to be produced
        // including the first time
        if last_snapshot + SNAPSHOT_MAX_AGE <= Instant::now() {
            let snapshot_rpc_http_url = config.snapshot.rpc_http_url.clone();
            // TODO split gMA + gPA
            // let snapshot = crate::snapshot::get_snapshot(snapshot_rpc_http_url.clone(), filter_config).await;
            let response =
                get_snapshot_gma(snapshot_rpc_http_url.clone(), account_ids.clone()).await;
            let snapshot = response.context("gma snapshot response").map_err_anyhow();
            match snapshot {
                Ok(SnapshotMultipleAccounts { snapshot_slot, snapshot_accounts }) => {
                    debug!(
                        "fetched new snapshot slot={} len={:?} time={:?}",
                        snapshot_slot,
                        snapshot_accounts.len(),
                        Instant::now() - SNAPSHOT_MAX_AGE - last_snapshot
                );
                    sender
                        .send(WebsocketMessage::SnapshotUpdate((snapshot_slot, snapshot_accounts)))
                        .await
                        .expect("sending must succeed");
                }
                Err(err) => {
                    if debouncer_errorlog.can_fire() {
                        warn!("failed to parse snapshot from rpc url {}: {}", snapshot_rpc_http_url.clone(), err);
                    }
                }
            }
            last_snapshot = Instant::now();
        }

        tokio::select! {
            account = account_subs.next() => {
                match account {
                    Some((account_id, response)) => {
                        sender.send(
                            WebsocketMessage::SingleUpdate(
                                response
                                    .map( |r: Response<UiAccount>|
                                        Response {
                                            context: r.context,
                                            value: RpcKeyedAccount {
                                                pubkey: account_id.clone(),
                                                account: r.value }})
                                    .map_err_anyhow()?)).await.expect("sending must succeed");
                    },
                    None => {
                        if debouncer_errorlog.can_fire() {
                            warn!("account stream closed");
                        }
                        // that is weired - assume that it is okey to cancel in all cases
                        // if filter_config.program_ids.is_empty() {
                        //     return Ok(());
                        // }
                        return Ok(());
                    },
                }
            },
            slot_update = slot_sub.next() => {
                match slot_update {
                    Some(slot_update) => {
                        sender.send(WebsocketMessage::SlotUpdate(slot_update.map_err_anyhow()?)).await.expect("sending must succeed");
                    },
                    None => {
                        warn!("slot update stream closed");
                        return Ok(());
                    },
                }
            },
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                warn!("websocket timeout");
                return Ok(())
            }
        }

    }
}

async fn feed_data_by_program(
    config: &SourceConfig,
    program_id: String,
    sender: async_channel::Sender<WebsocketMessage>,
    debouncer_errorlog: Arc<Debouncer>,
) -> anyhow::Result<()> {

    let rpc_ws_url: &str = &config.rpc_ws_url;

    let connect = ws::try_connect::<RpcSolPubSubClient>(rpc_ws_url).map_err_anyhow()?;
    let timeout = timeout(WS_CONNECT_TIMEOUT, connect).await?;
    let client = timeout.map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::processed()),
        data_slice: None,
        min_context_slot: None,
    };
    let program_accounts_config = RpcProgramAccountsConfig {
        filters: None,
        with_context: Some(true),
        account_config: account_info_config.clone(),
    };

    let mut program_subs = SelectAll::new();

    info!("FilterConfig specified program_ids, ignoring account_ids");
    program_subs.push(
        client
            .program_subscribe(program_id.clone(), Some(program_accounts_config.clone()))
            .map_err_anyhow()?,
    );

    let mut slot_sub = client.slots_updates_subscribe().map_err_anyhow()?;

    let mut last_snapshot = Instant::now().sub(SNAPSHOT_MAX_AGE);

    // consume from channels unitl an error happens
    loop {

        // occasionally cause a new snapshot to be produced
        // including the first time
        if last_snapshot + SNAPSHOT_MAX_AGE <= Instant::now() {
            let snapshot_rpc_http_url = config.snapshot.rpc_http_url.clone();
            // let snapshot = crate::snapshot::get_snapshot(snapshot_rpc_http_url.clone(), filter_config).await;
            let response =
                get_snapshot_gpa(snapshot_rpc_http_url.clone(), program_id.clone()).await;
            let snapshot = response.context("gpa snapshot response").map_err_anyhow();
            match snapshot {
                Ok(SnapshotProgramAccounts { snapshot_slot, snapshot_accounts } ) => {
                    let accounts: Vec<(String, Option<UiAccount>)> = snapshot_accounts
                        .iter()
                        .map(|x| {
                            let deref = x.clone();
                            (deref.pubkey, Some(deref.account))
                        })
                        .collect();
                    debug!(
                        "fetched new snapshot slot={} len={:?} time={:?}",
                        snapshot_slot,
                        accounts.len(),
                        Instant::now() - SNAPSHOT_MAX_AGE - last_snapshot
                    );
                    sender
                        .send(WebsocketMessage::SnapshotUpdate((snapshot_slot, accounts)))
                        .await
                        .expect("sending must succeed");
                }
                Err(err) => {
                    if debouncer_errorlog.can_fire() {
                        warn!("failed to parse snapshot from rpc url {}: {}", snapshot_rpc_http_url.clone(), err);
                    }
                }
            }
            last_snapshot = Instant::now();
        }

        tokio::select! {
            program_account = program_subs.next() => {
                match program_account {
                    Some(account) => {
                        sender.send(WebsocketMessage::SingleUpdate(account.map_err_anyhow()?)).await.expect("sending must succeed");
                    },
                    None => {
                        warn!("program account stream closed");
                        return Ok(());
                    },
                }
            },
            slot_update = slot_sub.next() => {
                match slot_update {
                    Some(slot_update) => {
                        sender.send(WebsocketMessage::SlotUpdate(slot_update.map_err_anyhow()?)).await.expect("sending must succeed");
                    },
                    None => {
                        warn!("slot update stream closed");
                        return Ok(());
                    },
                }
            },
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                warn!("websocket timeout");
                return Ok(())
            }
        }
    }
}

// TODO: rename / split / rework
pub async fn process_events(
    config: &SourceConfig,
    filter_config: &FilterConfig,
    account_write_queue_sender: async_channel::Sender<AccountWrite>,
    slot_queue_sender: async_channel::Sender<SlotUpdate>,
) {
    // Subscribe to program account updates websocket
    let (update_sender, update_receiver) = async_channel::unbounded::<WebsocketMessage>();
    info!("using config {config:?}");
    let config = config.clone();
    let filter_config = filter_config.clone();
    tokio::spawn(async move {
        // if the websocket disconnects, we get no data in a while etc, reconnect and try again
        let debouncer_errorlog = Arc::new( Debouncer::new(Duration::from_millis(500)));
        loop {
            let out = feed_data(&config, &filter_config, update_sender.clone(),
                                debouncer_errorlog.clone());
            if let Err(err) = out.await {
                if debouncer_errorlog.can_fire() {
                    warn!("feed data error: {}", err);
                }
            }
        }
    });

    //
    // The thread that pulls updates and forwards them to postgres
    //

    // consume websocket updates from rust channels
    loop {
        let update = update_receiver.recv().await.unwrap();
        trace!("got update message");

        match update {
            WebsocketMessage::SingleUpdate(update) => {
                trace!("single update");
                let account: Account = update.value.account.decode().unwrap();
                let pubkey = Pubkey::from_str(&update.value.pubkey).unwrap();
                account_write_queue_sender
                    .send(AccountWrite::from(pubkey, update.context.slot, 0, account))
                    .await
                    .expect("send success");
            }
            WebsocketMessage::SnapshotUpdate((slot, accounts)) => {
                trace!("snapshot update {slot}");
                for (pubkey, account) in accounts {
                    if let Some(account) = account {
                        let pubkey = Pubkey::from_str(&pubkey).unwrap();
                        account_write_queue_sender
                            .send(AccountWrite::from(
                                pubkey,
                                slot,
                                0,
                                account.decode().unwrap(),
                            ))
                            .await
                            .expect("send success");
                    }
                }
            }
            WebsocketMessage::SlotUpdate(update) => {
                trace!("slot update");
                let message = match *update {
                    solana_client::rpc_response::SlotUpdate::CreatedBank {
                        slot, parent, ..
                    } => Some(SlotUpdate {
                        slot,
                        parent: Some(parent),
                        status: SlotStatus::Processed,
                    }),
                    solana_client::rpc_response::SlotUpdate::OptimisticConfirmation {
                        slot,
                        ..
                    } => Some(SlotUpdate {
                        slot,
                        parent: None,
                        status: SlotStatus::Confirmed,
                    }),
                    solana_client::rpc_response::SlotUpdate::Root { slot, .. } => {
                        Some(SlotUpdate {
                            slot,
                            parent: None,
                            status: SlotStatus::Rooted,
                        })
                    }
                    _ => None,
                };
                if let Some(message) = message {
                    slot_queue_sender.send(message).await.expect("send success");
                }
            }
        }
    }
}
