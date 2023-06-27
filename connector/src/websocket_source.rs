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

use crate::{
    chain_data::SlotStatus, snapshot::get_snapshot, AccountWrite, AnyhowWrap, FilterConfig,
    SlotUpdate, SourceConfig,
};

enum WebsocketMessage {
    SingleUpdate(Response<RpcKeyedAccount>),
    SnapshotUpdate((Slot, Vec<(String, Option<UiAccount>)>)),
    SlotUpdate(Arc<solana_client::rpc_response::SlotUpdate>),
}

// TODO: the reconnecting should be part of this
async fn feed_data(
    config: &SourceConfig,
    filter_config: &FilterConfig,
    sender: async_channel::Sender<WebsocketMessage>,
) -> anyhow::Result<()> {
    debug!("feed_data {config:?}");

    let snapshot_duration = Duration::from_secs(300);

    let connect = ws::try_connect::<RpcSolPubSubClient>(&config.rpc_ws_url).map_err_anyhow()?;
    let client = connect.await.map_err_anyhow()?;

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
    let mut program_subs = SelectAll::new();

    if filter_config.program_ids.is_empty() {
        for account_id in filter_config.account_ids.clone() {
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
    } else {
        info!("FilterConfig specified program_ids, ignoring account_ids");
        for program_id in filter_config.program_ids.clone() {
            program_subs.push(
                client
                    .program_subscribe(program_id, Some(program_accounts_config.clone()))
                    .map_err_anyhow()?,
            );
        }
    }

    let mut slot_sub = client.slots_updates_subscribe().map_err_anyhow()?;

    let mut last_snapshot = Instant::now().checked_sub(snapshot_duration).unwrap();

    loop {
        // occasionally cause a new snapshot to be produced
        // including the first time
        if last_snapshot + snapshot_duration <= Instant::now() {
            let snapshot = get_snapshot(config.snapshot.rpc_http_url.clone(), filter_config).await;
            if let Ok((slot, accounts)) = snapshot {
                debug!(
                    "fetched new snapshot slot={slot} len={:?} time={:?}",
                    accounts.len(),
                    Instant::now() - snapshot_duration - last_snapshot
                );
                sender
                    .send(WebsocketMessage::SnapshotUpdate((slot, accounts)))
                    .await
                    .expect("sending must succeed");
            } else {
                error!("failed to parse snapshot")
            }
            last_snapshot = Instant::now();
        }

        if filter_config.program_ids.is_empty() {
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
                            warn!("account stream closed");
                            if filter_config.program_ids.is_empty() {
                                return Ok(());
                            }
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
        } else {
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
    let config = config.clone();
    let filter_config = filter_config.clone();
    tokio::spawn(async move {
        // if the websocket disconnects, we get no data in a while etc, reconnect and try again
        loop {
            let out = feed_data(&config, &filter_config, update_sender.clone());
            let _ = out.await;
        }
    });

    //
    // The thread that pulls updates and forwards them to postgres
    //

    // copy websocket updates into the postgres account write queue
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
