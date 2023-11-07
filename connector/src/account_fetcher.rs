use anyhow::{anyhow, Context};
use solana_sdk::account::AccountSharedData;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use std::any;

#[async_trait::async_trait]
pub trait AccountFetcherFeeds: Sync + Send {
    async fn feeds_fetch_raw_account(
        &self,
        address: &Pubkey,
    ) -> anyhow::Result<(AccountSharedData, Slot)>;

    async fn feeds_fetch_program_accounts(
        &self,
        program: &Pubkey,
        discriminator: [u8; 8],
    ) -> anyhow::Result<(Vec<(Pubkey, AccountSharedData)>, Slot)>;
}
