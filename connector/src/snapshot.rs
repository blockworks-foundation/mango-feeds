use std::sync::Arc;
use anyhow::anyhow;
use jsonrpc_core_client::transports::http;
use log::*;
use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_client::{
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_response::{OptionalContext, RpcKeyedAccount},
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_rpc::rpc::rpc_accounts::AccountsDataClient;
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};

use crate::{AnyhowWrap, FilterConfig};

pub async fn get_snapshot_gpa(
    rpc_http_url: String,
    program_id: String,
) -> anyhow::Result<OptionalContext<Vec<RpcKeyedAccount>>> {
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
    Ok(account_snapshot)
}

pub async fn get_snapshot_gma(
    rpc_http_url: String,
    ids: Vec<String>,
) -> anyhow::Result<solana_client::rpc_response::Response<Vec<Option<UiAccount>>>> {
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
    let account_snapshot = rpc_client
        .get_multiple_accounts(ids.clone(), Some(account_info_config))
        .await
        .map_err_anyhow()?;
    info!("snapshot received {:?}", ids);
    Ok(account_snapshot)
}

pub async fn get_snapshot(
    rpc_http_url: String,
    filter_config: &FilterConfig,
) -> anyhow::Result<(Slot, Vec<(String, Option<UiAccount>)>)> {
    if !filter_config.account_ids.is_empty() {
        let response =
            get_snapshot_gma(rpc_http_url.clone(), filter_config.account_ids.clone()).await;
        if let Ok(snapshot) = response {
            let accounts: Vec<(String, Option<UiAccount>)> = filter_config
                .account_ids
                .iter()
                .zip(snapshot.value)
                .map(|x| (x.0.clone(), x.1))
                .collect();
            Ok((snapshot.context.slot, accounts))
        } else {
            Err(anyhow!("invalid gma response {:?}", response))
        }
    } else if !filter_config.program_ids.is_empty() {
        let response =
            get_snapshot_gpa(rpc_http_url.clone(), filter_config.program_ids[0].clone()).await;
        if let Ok(OptionalContext::Context(snapshot)) = response {
            let accounts: Vec<(String, Option<UiAccount>)> = snapshot
                .value
                .iter()
                .map(|x| {
                    let deref = x.clone();
                    (deref.pubkey, Some(deref.account))
                })
                .collect();
            Ok((snapshot.context.slot, accounts))
        } else {
            Err(anyhow!("invalid gpa response {:?}", response))
        }
    } else {
        Err(anyhow!("invalid filter_config"))
    }
}
