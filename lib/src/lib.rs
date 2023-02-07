
pub mod account_write_filter;
pub mod chain_data;
pub mod fill_event_filter;
pub mod fill_event_postgres_target;
pub mod grpc_plugin_source;
pub mod memory_target;
pub mod metrics;
pub mod orderbook_filter;
pub mod postgres_types_numeric;
pub mod serum;
pub mod websocket_source;

pub use chain_data::SlotStatus;
use serde::{ser::SerializeStruct, Serialize, Serializer};

use {
    serde_derive::Deserialize,
    solana_sdk::{account::Account, pubkey::Pubkey},
};

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
            slot: slot,
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
pub struct PostgresConfig {
    pub connection_string: String,
    /// Number of parallel postgres connections used for insertions
    pub connection_count: u64,
    /// Maximum batch size for inserts over one connection
    pub max_batch_size: usize,
    /// Max size of queues
    pub max_queue_size: usize,
    /// Number of queries retries before fatal error
    pub retry_query_max_count: u64,
    /// Seconds to sleep between query retries
    pub retry_query_sleep_secs: u64,
    /// Seconds to sleep between connection attempts
    pub retry_connection_sleep_secs: u64,
    /// Fatal error when the connection can't be reestablished this long
    pub fatal_connection_timeout_secs: u64,
    /// Allow invalid TLS certificates, passed to native_tls danger_accept_invalid_certs
    pub allow_invalid_certs: bool,
    pub tls: Option<PostgresTlsConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PostgresTlsConfig {
    /// CA Cert file or env var
    pub ca_cert_path: String,
    /// PKCS12 client cert path
    pub client_key_path: String,
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
pub struct FilterConfig {
    pub program_ids: Vec<String>,
    pub account_ids: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct StatusResponse<'a> {
    pub success: bool,
    pub message: &'a str,
}

impl<'a> Serialize for StatusResponse<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Status", 2)?;
        state.serialize_field("success", &self.success)?;
        state.serialize_field("message", &self.message)?;

        state.end()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct SnapshotSourceConfig {
    pub rpc_http_url: String,
    pub program_id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetricsConfig {
    pub output_stdout: bool,
    pub output_http: bool,
    // TODO: add configurable port and endpoint url
    // TODO: add configurable write interval
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub postgres_target: PostgresConfig,
    pub source: SourceConfig,
    pub metrics: MetricsConfig,
}
