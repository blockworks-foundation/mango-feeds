use jsonrpc_core_client::transports::local;
use jsonrpc_core::futures::{self, future};
use jsonrpc_core::{IoHandler, Result, BoxFuture};
use jsonrpc_derive::rpc;
use solana_account_decoder::UiAccount;
use solana_rpc_client_api::config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_rpc_client_api::response::{OptionalContext, RpcKeyedAccount};
use solana_rpc_client_api::response::Response as RpcResponse;

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

