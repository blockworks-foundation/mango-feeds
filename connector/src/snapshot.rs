use log::*;
use serde_json::json;
use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_request::RpcRequest,
    rpc_response::{OptionalContext, Response, RpcKeyedAccount},
};
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};

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
    let rpc_client = RpcClient::new(rpc_http_url);

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
        .send::<OptionalContext<Vec<RpcKeyedAccount>>>(
            RpcRequest::GetProgramAccounts,
            json!([program_id, program_accounts_config]),
        )
        .await?;
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
    rpc_http_url: String,
    ids: Vec<String>,
) -> anyhow::Result<SnapshotMultipleAccounts> {
    let rpc_client = RpcClient::new(rpc_http_url);

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::finalized()),
        data_slice: None,
        min_context_slot: None,
    };

    info!("requesting gma snapshot {:?}", ids);
    let account_snapshot_response: Response<Vec<Option<UiAccount>>> = rpc_client
        .send(
            RpcRequest::GetMultipleAccounts,
            json!([ids, account_info_config]),
        )
        .await?;
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
