#![allow(unused_variables)]

use clap::Parser;

use jsonrpc_core_client::transports::http;
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_response::OptionalContext;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use mango_feeds_connector::snapshot::get_snapshot_gma;

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

    get_snapshot_gma(&rpc_http_url, vec![program_id.to_string()]).await?;


    Ok(())
}
