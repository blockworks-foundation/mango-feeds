use jsonrpc_core::Result;
use jsonrpc_derive::rpc;
use jsonrpc_pubsub::typed::Subscriber;
use solana_account_decoder::UiAccount;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use solana_rpc_client_api::response::Response as RpcResponse;
use jsonrpc_pubsub::SubscriptionId as PubSubSubscriptionId;

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

    // Get notification every time account data is changed
    // Accepts pubkey parameter as base-58 encoded string
    #[pubsub(
        subscription = "accountNotification",
        subscribe,
        name = "accountSubscribe"
    )]
    fn account_subscribe(
        &self,
        meta: Self::Metadata,
        subscriber: Subscriber<RpcResponse<UiAccount>>,
        pubkey_str: String,
        config: Option<RpcAccountInfoConfig>,
    );

    // Unsubscribe from account notification subscription.
    #[pubsub(
        subscription = "accountNotification",
        unsubscribe,
        name = "accountUnsubscribe"
    )]
    fn account_unsubscribe(
        &self,
        meta: Option<Self::Metadata>,
        id: PubSubSubscriptionId,
    ) -> Result<bool>;

}

