use jsonrpc_core::Result;
use jsonrpc_derive::rpc;
use solana_account_decoder::UiAccount;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use solana_rpc_client_api::response::Response as RpcResponse;

/// this definition is derived from solana-rpc/rpc.rs
/// we want to avoid the heavy dependency to solana-rpc
/// the crate solana-rpc-client provides some client methods but do not expose the ```Context```we need
#[rpc]
pub trait Rpc {
    type Metadata;

    #[rpc(meta, name = "getMultipleAccounts")]
    fn get_multiple_accounts(
        &self,
        meta: Self::Metadata,
        pubkey_strs: Vec<String>,
        config: Option<RpcAccountInfoConfig>,
    ) -> Result<RpcResponse<Vec<Option<UiAccount>>>>;

}

