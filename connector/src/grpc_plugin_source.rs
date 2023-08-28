use futures::stream::once;
use jsonrpc_core::futures::StreamExt;

use solana_account_decoder::UiAccount;
use solana_client::rpc_response::OptionalContext;
use solana_sdk::{account::Account, pubkey::Pubkey};

use futures::{future, future::FutureExt};
use yellowstone_grpc_proto::tonic::{
    metadata::MetadataValue,
    transport::{Certificate, Channel, ClientTlsConfig, Identity},
    Request,
};

use log::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{collections::HashMap, env, str::FromStr, time::Duration};

use yellowstone_grpc_proto::prelude::{
    geyser_client::GeyserClient, subscribe_update, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots, SubscribeUpdate,
    SubscribeUpdateSlotStatus,
};

use crate::snapshot::{get_snapshot_gma, get_snapshot_gpa};
use crate::{EntityFilter, FilterConfig};
use crate::{
    chain_data::SlotStatus,
    metrics::{MetricType, Metrics},
    AccountWrite, GrpcSourceConfig, SlotUpdate, SnapshotSourceConfig, SourceConfig, TlsConfig,
};

struct SnapshotData {
    slot: u64,
    accounts: Vec<(String, Option<UiAccount>)>,
}
enum Message {
    GrpcUpdate(SubscribeUpdate),
    Snapshot(SnapshotData),
}

async fn feed_data_geyser(
    grpc_config: &GrpcSourceConfig,
    tls_config: Option<ClientTlsConfig>,
    snapshot_config: &SnapshotSourceConfig,
    filter_config: &FilterConfig,
    sender: async_channel::Sender<Message>,
) -> anyhow::Result<()> {
    let grpc_connection_string = match &grpc_config.connection_string.chars().next().unwrap() {
        '$' => env::var(&grpc_config.connection_string[1..])
            .expect("reading connection string from env"),
        _ => grpc_config.connection_string.clone(),
    };
    let snapshot_rpc_http_url = match &snapshot_config.rpc_http_url.chars().next().unwrap() {
        '$' => env::var(&snapshot_config.rpc_http_url[1..])
            .expect("reading connection string from env"),
        _ => snapshot_config.rpc_http_url.clone(),
    };
    info!("connecting {}", grpc_connection_string);
    let endpoint = Channel::from_shared(grpc_connection_string)?;
    let channel = if let Some(tls) = tls_config {
        endpoint.tls_config(tls)?
    } else {
        endpoint
    }
    .connect()
    .await?;
    let token: Option<MetadataValue<_>> = match &grpc_config.token {
        Some(token) => match token.chars().next().unwrap() {
            '$' => Some(
                env::var(&token[1..])
                    .expect("reading token from env")
                    .parse()?,
            ),
            _ => Some(token.clone().parse()?),
        },
        None => None,
    };
    let mut client = GeyserClient::with_interceptor(channel, move |mut req: Request<()>| {
        if let Some(token) = &token {
            req.metadata_mut().insert("x-token", token.clone());
        }
        Ok(req)
    });

    let mut accounts = HashMap::new();
    let mut slots = HashMap::new();
    let blocks = HashMap::new();
    let transactions = HashMap::new();
    let blocks_meta = HashMap::new();

    match &filter_config.entity_filter {
        EntityFilter::FilterByProgramId(program_id) => {
            // note: the v0.1 (commit 2925926) logic allowed to have one program_id (the owner) plus account_ids which is not allowed with the new filter design

            accounts.insert(
                "client".to_owned(),
                SubscribeRequestFilterAccounts {
                    account: vec![],
                    owner: vec![program_id.clone()],
                    filters: vec![],
                },
            );
        }
        EntityFilter::FilterByAccountIds(account_ids) => {
            accounts.insert(
                "client".to_owned(),
                SubscribeRequestFilterAccounts {
                    account: account_ids.clone(),
                    owner: vec![],
                    filters: vec![],
                },
            );
        }
    }

    slots.insert("client".to_owned(), SubscribeRequestFilterSlots {});

    let request = SubscribeRequest {
        accounts,
        blocks,
        blocks_meta,
        slots,
        transactions,
    };
    info!("Going to send request: {:?}", request);

    let response = client.subscribe(once(async move { request })).await?;
    let mut update_stream = response.into_inner();

    // We can't get a snapshot immediately since the finalized snapshot would be for a
    // slot in the past and we'd be missing intermediate updates.
    //
    // Delay the request until the first slot we received all writes for becomes rooted
    // to avoid that problem - partially. The rooted slot will still be larger than the
    // finalized slot, so add a number of slots as a buffer.
    //
    // If that buffer isn't sufficient, there'll be a retry.

    // The first slot that we will receive _all_ account writes for
    let mut first_full_slot: u64 = u64::MAX;

    // If a snapshot should be performed when ready.
    let mut snapshot_needed = true;

    // The highest "rooted" slot that has been seen.
    let mut max_rooted_slot = 0;

    // Data for slots will arrive out of order. This value defines how many
    // slots after a slot was marked "rooted" we assume it'll not receive
    // any more account write information.
    //
    // This is important for the write_version mapping (to know when slots can
    // be dropped).
    let max_out_of_order_slots = 40;

    // Number of slots that we expect "finalized" commitment to lag
    // behind "rooted". This matters for getProgramAccounts based snapshots,
    // which will have "finalized" commitment.
    let mut rooted_to_finalized_slots = 30;

    let mut snapshot_gma = future::Fuse::terminated();
    let mut snapshot_gpa = future::Fuse::terminated();

    // The plugin sends a ping every 5s or so
    let fatal_idle_timeout = Duration::from_secs(60);

    // Highest slot that an account write came in for.
    let mut newest_write_slot: u64 = 0;

    #[derive(Clone, Debug)]
    struct WriteVersion {
        // Write version seen on-chain
        global: u64,
        // The per-pubkey per-slot write version
        slot: u32,
    }

    // map slot -> (pubkey -> WriteVersion)
    //
    // Since the write_version is a private indentifier per node it can't be used
    // to deduplicate events from multiple nodes. Here we rewrite it such that each
    // pubkey and each slot has a consecutive numbering of writes starting at 1.
    //
    // That number will be consistent for each node.
    let mut slot_pubkey_writes = HashMap::<u64, HashMap<[u8; 32], WriteVersion>>::new();

    loop {
        tokio::select! {
            update = update_stream.next() => {
                use subscribe_update::UpdateOneof;
                let mut update = update.ok_or(anyhow::anyhow!("geyser plugin has closed the stream"))??;
                match update.update_oneof.as_mut().expect("invalid grpc") {
                    UpdateOneof::Slot(slot_update) => {
                        let status = slot_update.status;
                        if status == SubscribeUpdateSlotStatus::Finalized as i32 {
                            if first_full_slot == u64::MAX {
                                // TODO: is this equivalent to before? what was highesy_write_slot?
                                first_full_slot = slot_update.slot + 1;
                            }
                            if slot_update.slot > max_rooted_slot {
                                max_rooted_slot = slot_update.slot;

                                // drop data for slots that are well beyond rooted
                                slot_pubkey_writes.retain(|&k, _| k >= max_rooted_slot - max_out_of_order_slots);
                            }

                            if snapshot_needed && max_rooted_slot - rooted_to_finalized_slots > first_full_slot {
                                snapshot_needed = false;
                                match &filter_config.entity_filter {
                                    EntityFilter::FilterByAccountIds(account_ids) => {
                                        snapshot_gma = tokio::spawn(get_snapshot_gma(snapshot_rpc_http_url.clone(), account_ids.clone())).fuse();
                                    },
                                    EntityFilter::FilterByProgramId(program_id) => {
                                        snapshot_gpa = tokio::spawn(get_snapshot_gpa(snapshot_rpc_http_url.clone(), program_id.clone())).fuse();
                                    },
                                };
                            }
                        }
                    },
                    UpdateOneof::Account(info) => {
                        if info.slot < first_full_slot {
                            // Don't try to process data for slots where we may have missed writes:
                            // We could not map the write_version correctly for them.
                            continue;
                        }

                        if info.slot > newest_write_slot {
                            newest_write_slot = info.slot;
                        } else if max_rooted_slot > 0 && info.slot < max_rooted_slot - max_out_of_order_slots {
                            anyhow::bail!("received write {} slots back from max rooted slot {}", max_rooted_slot - info.slot, max_rooted_slot);
                        }

                        let pubkey_writes = slot_pubkey_writes.entry(info.slot).or_default();
                        let mut write = match info.account.clone() {
                            Some(x) => x,
                            None => {
                                // TODO: handle error
                                continue;
                            },
                        };

                        let pubkey_bytes = Pubkey::try_from(write.pubkey).unwrap().to_bytes();
                        let write_version_mapping = pubkey_writes.entry(pubkey_bytes).or_insert(WriteVersion {
                            global: write.write_version,
                            slot: 1, // write version 0 is reserved for snapshots
                        });

                        // We assume we will receive write versions for each pubkey in sequence.
                        // If this is not the case, logic here does not work correctly because
                        // a later write could arrive first.
                        if write.write_version < write_version_mapping.global {
                            anyhow::bail!("unexpected write version: got {}, expected >= {}", write.write_version, write_version_mapping.global);
                        }

                        // Rewrite the update to use the local write version and bump it
                        write.write_version = write_version_mapping.slot as u64;
                        write_version_mapping.slot += 1;
                    },
                    UpdateOneof::Block(_) => {},
                    UpdateOneof::Transaction(_) => {},
                    UpdateOneof::BlockMeta(_) => {},
                    UpdateOneof::Ping(_) => {},
                }
                sender.send(Message::GrpcUpdate(update)).await.expect("send success");
            },
            snapshot = &mut snapshot_gma => {
                let snapshot = snapshot??;
                info!("snapshot is for slot {}, first full slot was {}", snapshot.snapshot_slot, first_full_slot);
                if snapshot.snapshot_slot >= first_full_slot {
                    sender.send(Message::Snapshot(SnapshotData {
                        accounts: snapshot.snapshot_accounts,
                        slot: snapshot.snapshot_slot,
                    }))
                    .await
                    .expect("send success");
                } else {
                    info!(
                        "snapshot is too old: has slot {}, expected {} minimum",
                        snapshot.snapshot_slot,
                        first_full_slot
                    );
                    // try again in another 10 slots
                    snapshot_needed = true;
                    rooted_to_finalized_slots += 10;
                }
            },
            snapshot = &mut snapshot_gpa => {
                let snapshot = snapshot??;
                info!("snapshot is for slot {}, first full slot was {}", snapshot.snapshot_slot, first_full_slot);
                if snapshot.snapshot_slot >= first_full_slot {
                    let accounts: Vec<(String, Option<UiAccount>)> = snapshot.snapshot_accounts.iter().map(|x| {
                        let deref = x.clone();
                        (deref.pubkey, Some(deref.account))
                    }).collect();
                    sender
                    .send(Message::Snapshot(SnapshotData {
                        accounts,
                        slot: snapshot.snapshot_slot,
                    }))
                    .await
                    .expect("send success");
                } else {
                    info!(
                        "snapshot is too old: has slot {}, expected {} minimum",
                        snapshot.snapshot_slot,
                        first_full_slot
                    );
                    // try again in another 10 slots
                    snapshot_needed = true;
                    rooted_to_finalized_slots += 10;
                }
            },
            _ = tokio::time::sleep(fatal_idle_timeout) => {
                anyhow::bail!("geyser plugin hasn't sent a message in too long");
            }
        }
    }
}

fn make_tls_config(config: &TlsConfig) -> ClientTlsConfig {
    let server_root_ca_cert = match &config.ca_cert_path.chars().next().unwrap() {
        '$' => env::var(&config.ca_cert_path[1..])
            .expect("reading server root ca cert from env")
            .into_bytes(),
        _ => std::fs::read(&config.ca_cert_path).expect("reading server root ca cert from file"),
    };
    let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert);
    let client_cert = match &config.client_cert_path.chars().next().unwrap() {
        '$' => env::var(&config.client_cert_path[1..])
            .expect("reading client cert from env")
            .into_bytes(),
        _ => std::fs::read(&config.client_cert_path).expect("reading client cert from file"),
    };
    let client_key = match &config.client_key_path.chars().next().unwrap() {
        '$' => env::var(&config.client_key_path[1..])
            .expect("reading client key from env")
            .into_bytes(),
        _ => std::fs::read(&config.client_key_path).expect("reading client key from file"),
    };
    let client_identity = Identity::from_pem(client_cert, client_key);
    let domain_name = match &config.domain_name.chars().next().unwrap() {
        '$' => env::var(&config.domain_name[1..]).expect("reading domain name from env"),
        _ => config.domain_name.clone(),
    };
    ClientTlsConfig::new()
        .ca_certificate(server_root_ca_cert)
        .identity(client_identity)
        .domain_name(domain_name)
}

pub async fn process_events(
    config: &SourceConfig,
    filter_config: &FilterConfig,
    account_write_queue_sender: async_channel::Sender<AccountWrite>,
    slot_queue_sender: async_channel::Sender<SlotUpdate>,
    metrics_sender: Metrics,
    exit: Arc<AtomicBool>,
) {
    // Subscribe to geyser
    let (msg_sender, msg_receiver) = async_channel::bounded::<Message>(config.dedup_queue_size);
    for grpc_source in config.grpc_sources.clone() {
        let msg_sender = msg_sender.clone();
        let snapshot_source = config.snapshot.clone();
        let metrics_sender = metrics_sender.clone();
        let f = filter_config.clone();

        // Make TLS config if configured
        let tls_config = grpc_source.tls.as_ref().map(make_tls_config).or_else(|| {
            if grpc_source.connection_string.starts_with("https") {
                Some(ClientTlsConfig::new())
            } else {
                None
            }
        });

        tokio::spawn(async move {
            let mut metric_retries = metrics_sender.register_u64(
                format!("grpc_source_{}_connection_retries", grpc_source.name,),
                MetricType::Counter,
            );
            let metric_connected =
                metrics_sender.register_bool(format!("grpc_source_{}_status", grpc_source.name));

            // Continuously reconnect on failure
            loop {
                metric_connected.set(true);
                let out = feed_data_geyser(
                    &grpc_source,
                    tls_config.clone(),
                    &snapshot_source,
                    &f,
                    msg_sender.clone(),
                );
                let err = out.await.unwrap_err();
                warn!(
                    "error during communication with the geyser plugin. retrying. {:?}",
                    err
                );

                metric_connected.set(false);
                metric_retries.increment();

                tokio::time::sleep(std::time::Duration::from_secs(
                    grpc_source.retry_connection_sleep_secs,
                ))
                .await;
            }
        });
    }

    // slot -> (pubkey -> write_version)
    //
    // To avoid unnecessarily sending requests to SQL, we track the latest write_version
    // for each (slot, pubkey). If an already-seen write_version comes in, it can be safely
    // discarded.
    let mut latest_write = HashMap::<u64, HashMap<[u8; 32], u64>>::new();

    // Number of slots to retain in latest_write
    let latest_write_retention = 50;

    let mut metric_account_writes =
        metrics_sender.register_u64("grpc_account_writes".into(), MetricType::Counter);
    let mut metric_account_queue =
        metrics_sender.register_u64("grpc_account_write_queue".into(), MetricType::Gauge);
    let mut metric_dedup_queue =
        metrics_sender.register_u64("grpc_dedup_queue".into(), MetricType::Gauge);
    let mut metric_slot_queue =
        metrics_sender.register_u64("grpc_slot_update_queue".into(), MetricType::Gauge);
    let mut metric_slot_updates =
        metrics_sender.register_u64("grpc_slot_updates".into(), MetricType::Counter);
    let mut metric_snapshots =
        metrics_sender.register_u64("grpc_snapshots".into(), MetricType::Counter);
    let mut metric_snapshot_account_writes =
        metrics_sender.register_u64("grpc_snapshot_account_writes".into(), MetricType::Counter);

    loop {
        if exit.load(Ordering::Relaxed) {
            warn!("shutting down grpc_plugin_source...");
            break;
        }

        metric_dedup_queue.set(msg_receiver.len() as u64);
        let msg = msg_receiver.recv().await.expect("sender must not close");
        match msg {
            Message::GrpcUpdate(update) => {
                use subscribe_update::UpdateOneof;
                match update.update_oneof.expect("invalid grpc") {
                    UpdateOneof::Account(info) => {
                        let update = match info.account.clone() {
                            Some(x) => x,
                            None => {
                                // TODO: handle error
                                continue;
                            }
                        };
                        assert!(update.pubkey.len() == 32);
                        assert!(update.owner.len() == 32);

                        metric_account_writes.increment();
                        metric_account_queue.set(account_write_queue_sender.len() as u64);

                        // Skip writes that a different server has already sent
                        let pubkey_writes = latest_write.entry(info.slot).or_default();
                        let pubkey_bytes =
                            Pubkey::try_from(update.pubkey.clone()).unwrap().to_bytes();
                        let writes = pubkey_writes.entry(pubkey_bytes).or_insert(0);
                        if update.write_version <= *writes {
                            continue;
                        }
                        *writes = update.write_version;
                        latest_write.retain(|&k, _| k >= info.slot - latest_write_retention);
                        // let mut uncompressed: Vec<u8> = Vec::new();
                        // zstd_decompress(&update.data, &mut uncompressed).unwrap();
                        account_write_queue_sender
                            .send(AccountWrite {
                                pubkey: Pubkey::try_from(update.pubkey.clone()).unwrap(),
                                slot: info.slot,
                                write_version: update.write_version,
                                lamports: update.lamports,
                                owner: Pubkey::try_from(update.owner.clone()).unwrap(),
                                executable: update.executable,
                                rent_epoch: update.rent_epoch,
                                data: update.data,
                                // TODO: what should this be? related to account deletes?
                                is_selected: true,
                            })
                            .await
                            .expect("send success");
                    }
                    UpdateOneof::Slot(update) => {
                        metric_slot_updates.increment();
                        metric_slot_queue.set(slot_queue_sender.len() as u64);

                        let status =
                            SubscribeUpdateSlotStatus::from_i32(update.status).map(|v| match v {
                                SubscribeUpdateSlotStatus::Processed => SlotStatus::Processed,
                                SubscribeUpdateSlotStatus::Confirmed => SlotStatus::Confirmed,
                                SubscribeUpdateSlotStatus::Finalized => SlotStatus::Rooted,
                            });
                        if status.is_none() {
                            error!("unexpected slot status: {}", update.status);
                            continue;
                        }
                        let slot_update = SlotUpdate {
                            slot: update.slot,
                            parent: update.parent,
                            status: status.expect("qed"),
                        };

                        slot_queue_sender
                            .send(slot_update)
                            .await
                            .expect("send success");
                    }
                    UpdateOneof::Block(_) => {}
                    UpdateOneof::Transaction(_) => {}
                    UpdateOneof::BlockMeta(_) => {}
                    UpdateOneof::Ping(_) => {}
                }
            }
            Message::Snapshot(update) => {
                metric_snapshots.increment();
                info!("processing snapshot...");
                for account in update.accounts.iter() {
                    metric_snapshot_account_writes.increment();
                    metric_account_queue.set(account_write_queue_sender.len() as u64);

                    match account {
                        (key, Some(ui_account)) => {
                            // TODO: Resnapshot on invalid data?
                            let pubkey = Pubkey::from_str(key).unwrap();
                            let account: Account = ui_account.decode().unwrap();
                            account_write_queue_sender
                                .send(AccountWrite::from(pubkey, update.slot, 0, account))
                                .await
                                .expect("send success");
                        }
                        (key, None) => warn!("account not found {}", key),
                    }
                }
                info!("processing snapshot done");
            }
        }
    }
}
