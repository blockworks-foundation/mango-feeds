#![allow(unused_variables)]

use clap::Parser;

use jsonrpc_core_client::transports::http;
use mango_feeds_connector::solana_rpc_minimal::rpc_accounts_scan::RpcAccountsScanClient;
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_response::OptionalContext;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;

#[derive(Parser, Debug, Clone)]
#[clap()]
struct Cli {
    // e.g. https://mango.devnet.rpcpool.com
    #[clap(short, long, env)]
    rpc_url: String,

    // e.g. 4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg
    #[clap(short, long, env)]
    program_account: Pubkey,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    solana_logger::setup_with_default("info");

    let cli = Cli::parse_from(std::env::args_os());

    let rpc_http_url = cli.rpc_url;
    let program_id = cli.program_account;

    let rpc_client_scan = http::connect::<RpcAccountsScanClient>(&rpc_http_url)
        .await
        .unwrap();

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::finalized()),
        data_slice: None,
        min_context_slot: None,
    };

    let program_info_config = RpcProgramAccountsConfig {
        filters: None,
        account_config: account_info_config,
        with_context: Some(true),
    };

    let snapshot = rpc_client_scan
        .get_program_accounts(program_id.to_string(), Some(program_info_config))
        .await;
    if let OptionalContext::Context(snapshot_data) = snapshot.unwrap() {
        println!("api version: {:?}", snapshot_data.context.api_version);
        println!("#accounts {:?}", snapshot_data.value.len());
        // mainnet, mango accounts: #accounts 1971, 1.14.24
    }

    Ok(())
}
