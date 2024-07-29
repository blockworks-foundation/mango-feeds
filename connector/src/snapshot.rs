use jsonrpc_core_client::transports::http;
use log::*;
use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_client::{
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_response::{OptionalContext, RpcKeyedAccount},
};
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};

use crate::AnyhowWrap;
use crate::solana_rpc_minimal::rpc_accounts_scan::RpcAccountsScanClient;

/// gPA snapshot struct
pub struct SnapshotProgramAccounts {
    pub slot: Slot,
    pub accounts: Vec<RpcKeyedAccount>,
}

/// gMA snapshot struct
pub struct SnapshotMultipleAccounts {
    pub slot: Slot,
    // (account pubkey, snapshot account)
    pub accounts: Vec<(String, Option<UiAccount>)>,
}

pub async fn get_snapshot_gpa(
    rpc_http_url: String,
    program_id: String,
) -> anyhow::Result<SnapshotProgramAccounts> {
    let rpc_client = http::connect::<RpcAccountsScanClient>(&rpc_http_url)
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

    info!("requesting gpa snapshot {}", program_id);
    let account_snapshot = rpc_client
        .get_program_accounts(program_id.clone(), Some(program_accounts_config.clone()))
        .await
        .map_err_anyhow()?;
    info!("gpa snapshot received {}", program_id);

    match account_snapshot {
        OptionalContext::Context(snapshot) => {
            let snapshot_slot = snapshot.context.slot;
            Ok(SnapshotProgramAccounts {
                slot: snapshot_slot,
                accounts: snapshot.value,
            })
        }
        OptionalContext::NoContext(_) => anyhow::bail!("bad snapshot format"),
    }
}

pub async fn get_snapshot_gma(
    rpc_http_url: &str,
    ids: Vec<String>,
) -> anyhow::Result<SnapshotMultipleAccounts> {
    let rpc_client = http::connect::<RpcAccountsScanClient>(rpc_http_url)
        .await
        .map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::finalized()),
        data_slice: None,
        min_context_slot: None,
    };

    info!("requesting gma snapshot {:?}", ids);
    let account_snapshot_response = rpc_client
        .get_multiple_accounts(ids.clone(), Some(account_info_config))
        .await
        .map_err_anyhow()?;
    info!("gma snapshot received {:?}", ids);

    let first_full_shot = account_snapshot_response.context.slot;

    let acc: Vec<(String, Option<UiAccount>)> = ids
        .iter()
        .zip(account_snapshot_response.value)
        .map(|x| (x.0.clone(), x.1))
        .collect();
    Ok(SnapshotMultipleAccounts {
        slot: first_full_shot,
        accounts: acc,
    })
}
