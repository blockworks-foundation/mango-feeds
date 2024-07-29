

pub mod rpc_accounts_scan {
    use std::sync::Arc;
    use jsonrpc_core::Result;
    use jsonrpc_derive::rpc;
    use jsonrpc_pubsub::typed::Subscriber;
    use solana_account_decoder::UiAccount;
    use solana_rpc_client_api::config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
    use solana_rpc_client_api::response::{Response as RpcResponse, RpcKeyedAccount, SlotUpdate, OptionalContext};
    use jsonrpc_pubsub::SubscriptionId as PubSubSubscriptionId;

    /// this definition is derived from solana-rpc/rpc.rs
    /// we want to avoid the heavy dependency to solana-rpc
    /// the crate solana-rpc-client provides some client methods but do not expose the ```Context```we need
    ///
    #[rpc]
    pub trait RpcAccountsScan {
        type Metadata;

        #[rpc(meta, name = "getProgramAccounts")]
        fn get_program_accounts(
            &self,
            meta: Self::Metadata,
            program_id_str: String,
            config: Option<RpcProgramAccountsConfig>,
        ) -> Result<OptionalContext<Vec<RpcKeyedAccount>>>;

        #[rpc(meta, name = "getMultipleAccounts")]
        fn get_multiple_accounts(
            &self,
            meta: Self::Metadata,
            pubkey_strs: Vec<String>,
            config: Option<RpcAccountInfoConfig>,
        ) -> Result<RpcResponse<Vec<Option<UiAccount>>>>;


    }

}

pub mod rpc_pubsub {
    use std::sync::Arc;
    use jsonrpc_core::Result;
    use jsonrpc_derive::rpc;
    use jsonrpc_pubsub::typed::Subscriber;
    use solana_account_decoder::UiAccount;
    use solana_rpc_client_api::config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
    use solana_rpc_client_api::response::{Response as RpcResponse, RpcKeyedAccount, SlotUpdate, OptionalContext};
    use jsonrpc_pubsub::SubscriptionId as PubSubSubscriptionId;

    #[rpc]
    pub trait RpcSolPubSub {
        type Metadata;

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
        ) -> jsonrpc_core::Result<bool>;


        // Get notification every time account data owned by a particular program is changed
        // Accepts pubkey parameter as base-58 encoded string
        #[pubsub(
            subscription = "programNotification",
            subscribe,
            name = "programSubscribe"
        )]
        fn program_subscribe(
            &self,
            meta: Self::Metadata,
            subscriber: Subscriber<RpcResponse<RpcKeyedAccount>>,
            pubkey_str: String,
            config: Option<RpcProgramAccountsConfig>,
        );

        // Unsubscribe from account notification subscription.
        #[pubsub(
            subscription = "programNotification",
            unsubscribe,
            name = "programUnsubscribe"
        )]
        fn program_unsubscribe(
            &self,
            meta: Option<Self::Metadata>,
            id: PubSubSubscriptionId,
        ) -> jsonrpc_core::Result<bool>;


        // Get series of updates for all slots
        #[pubsub(
            subscription = "slotsUpdatesNotification",
            subscribe,
            name = "slotsUpdatesSubscribe"
        )]
        fn slots_updates_subscribe(
            &self,
            meta: Self::Metadata,
            subscriber: Subscriber<Arc<SlotUpdate>>,
        );

        // Unsubscribe from slots updates notification subscription.
        #[pubsub(
            subscription = "slotsUpdatesNotification",
            unsubscribe,
            name = "slotsUpdatesUnsubscribe"
        )]
        fn slots_updates_unsubscribe(
            &self,
            meta: Option<Self::Metadata>,
            id: PubSubSubscriptionId,
        ) -> jsonrpc_core::Result<bool>;

    }

}
