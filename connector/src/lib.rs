pub mod account_write_filter;
pub mod chain_data;
pub mod grpc_plugin_source;
pub mod metrics;
pub mod snapshot;
pub mod websocket_source;

use {
    serde_derive::Deserialize,
    solana_sdk::{account::Account, pubkey::Pubkey},
};

#[cfg(all(feature = "solana-1-14", feature = "solana-1-15"))]
compile_error!(
    "feature \"solana-1-14\" and feature \"solana-1-15\" cannot be enabled at the same time"
);

#[cfg(feature = "solana-1-14")]
use solana_rpc::rpc::rpc_accounts::AccountsDataClient as GetProgramAccountsClient;
#[cfg(feature = "solana-1-15")]
use solana_rpc::rpc::rpc_accounts_scan::AccountsScanClient as GetProgramAccountsClient;

pub use solana_sdk;

trait AnyhowWrap {
    type Value;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value>;
}

impl<T, E: std::fmt::Debug> AnyhowWrap for Result<T, E> {
    type Value = T;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value> {
        self.map_err(|err| anyhow::anyhow!("{:?}", err))
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct AccountWrite {
    pub pubkey: Pubkey,
    pub slot: u64,
    pub write_version: u64,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub is_selected: bool,
}

impl AccountWrite {
    fn from(pubkey: Pubkey, slot: u64, write_version: u64, account: Account) -> AccountWrite {
        AccountWrite {
            pubkey,
            slot,
            write_version,
            lamports: account.lamports,
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: account.data,
            is_selected: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SlotUpdate {
    pub slot: u64,
    pub parent: Option<u64>,
    pub status: chain_data::SlotStatus,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TlsConfig {
    pub ca_cert_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    pub domain_name: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GrpcSourceConfig {
    pub name: String,
    pub connection_string: String,
    pub retry_connection_sleep_secs: u64,
    pub tls: Option<TlsConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SourceConfig {
    pub dedup_queue_size: usize,
    pub grpc_sources: Vec<GrpcSourceConfig>,
    pub snapshot: SnapshotSourceConfig,
    pub rpc_ws_url: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SnapshotSourceConfig {
    pub rpc_http_url: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FilterConfig {
    pub program_ids: Vec<String>,
    pub account_ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetricsConfig {
    pub output_stdout: bool,
    pub output_http: bool,
    // TODO: add configurable port and endpoint url
    // TODO: add configurable write interval
}
