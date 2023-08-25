use anyhow::{anyhow, Context};
use jsonrpc_core_client::transports::http;
use log::*;
use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_client::{
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_response::{OptionalContext, RpcKeyedAccount},
};
use solana_rpc::rpc::rpc_accounts::AccountsDataClient;
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};

use crate::{AnyhowWrap, EntityFilter, FilterConfig};

/// gPA snapshot struct
pub struct SnapshotProgramAccounts {
    pub snapshot_slot: Slot,
    pub snapshot_accounts: Vec<RpcKeyedAccount>,
}

/// gMA snapshot struct
pub struct SnapshotMultipleAccounts {
    pub snapshot_slot: Slot,
    pub snapshot_accounts: Vec<(String, Option<UiAccount>)>,
}


pub async fn get_snapshot_gpa(
    rpc_http_url: String,
    program_id: String,
) -> anyhow::Result<SnapshotProgramAccounts> {
    let rpc_client = http::connect::<crate::GetProgramAccountsClient>(&rpc_http_url)
        .await
        .map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::finalized()),
        data_slice: None,
        min_context_slot: None,
    };
    let program_accounts_config = RpcProgramAccountsConfig {
        filters: None,
        with_context: Some(true),
        account_config: account_info_config.clone(),
    };

    info!("requesting snapshot {}", program_id);
    let account_snapshot = rpc_client
        .get_program_accounts(program_id.clone(), Some(program_accounts_config.clone()))
        .await
        .map_err_anyhow()?;
    info!("snapshot received {}", program_id);

    match account_snapshot {
        OptionalContext::Context(snapshot) => {
            let snapshot_slot = snapshot.context.slot;
            return Ok(SnapshotProgramAccounts {
                snapshot_slot,
                snapshot_accounts: snapshot.value,
            });
        }
        OptionalContext::NoContext(_) => { anyhow::bail!("bad snapshot format"); }
    }
}

pub async fn get_snapshot_gma(
    rpc_http_url: String,
    ids: Vec<String>,
) -> anyhow::Result<SnapshotMultipleAccounts> {
    let rpc_client = http::connect::<AccountsDataClient>(&rpc_http_url)
        .await
        .map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::finalized()),
        data_slice: None,
        min_context_slot: None,
    };

    info!("requesting snapshot {:?}", ids);
    let account_snapshot_response = rpc_client
        .get_multiple_accounts(ids.clone(), Some(account_info_config))
        .await
        .map_err_anyhow()?;
    info!("snapshot received {:?}", ids);

    let first_full_shot = account_snapshot_response.context.slot;

    let acc: Vec<(String, Option<UiAccount>)> = ids.iter().zip(account_snapshot_response.value).map(|x| (x.0.clone(), x.1)).collect();
    Ok(SnapshotMultipleAccounts {
        snapshot_slot: first_full_shot,
        snapshot_accounts: acc,
    })
}

pub async fn get_snapshot(
    rpc_http_url: String,
    filter_config: &FilterConfig,
) -> anyhow::Result<(Slot, Vec<(String, Option<UiAccount>)>)> {
    match &filter_config.entity_filter {
        EntityFilter::FilterByAccountIds(account_ids) => {
            let response =
                get_snapshot_gma(rpc_http_url.clone(), account_ids.clone()).await;
            let snapshot = response.context("gma snapshot response").map_err_anyhow()?;
            Ok((snapshot.snapshot_slot, snapshot.snapshot_accounts))
        }
        EntityFilter::FilterByProgramId(program_id) => {
            let response =
                get_snapshot_gpa(rpc_http_url.clone(), program_id.clone()).await;
            let snapshot = response.context("gpa snapshot response").map_err_anyhow()?;
            let accounts: Vec<(String, Option<UiAccount>)> = snapshot.snapshot_accounts
                .iter()
                .map(|x| {
                    let deref = x.clone();
                    (deref.pubkey, Some(deref.account))
                })
                .collect();
            Ok((snapshot.snapshot_slot, accounts))
        }
    }
}
