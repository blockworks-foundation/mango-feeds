use std::sync::{Arc, RwLock};

use crate::chain_data::*;


use anyhow::Context;

use solana_client::nonblocking::rpc_client::RpcClient as RpcClientAsync;
use solana_sdk::account::{AccountSharedData, ReadableAccount};
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

pub struct FeedsAccountFetcher {
    pub chain_data: Arc<RwLock<ChainData>>,
    // pub rpc: RpcClientAsync,
}

impl FeedsAccountFetcher {
    pub fn feeds_fetch_raw(&self, address: &Pubkey) -> anyhow::Result<AccountSharedData> {
        let chain_data = self.chain_data.read().unwrap();
        Ok(chain_data
            .account(address)
            .map(|d| d.account.clone())
            .with_context(|| format!("fetch account {} via chain_data", address))?)
    }
}
