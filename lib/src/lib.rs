pub mod fill_event_filter;
pub mod fill_event_postgres_target;
pub mod memory_target;
pub mod orderbook_filter;
pub mod postgres_types_numeric;
pub mod serum;

use serde::{ser::SerializeStruct, Serialize, Serializer};
use serde_derive::Deserialize;

pub use solana_geyser_connector_data_streams::*;

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
pub struct Config {
    pub postgres_target: PostgresConfig,
    pub source: SourceConfig,
    pub metrics: MetricsConfig,
}
