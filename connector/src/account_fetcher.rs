use std::any;
use anyhow::{anyhow, Context};
use crate::account_fetchers::{AccountFetcherFeeds, RpcAccountFetcher};
use solana_sdk::account::AccountSharedData;
use solana_sdk::pubkey::Pubkey;


#[async_trait::async_trait]
impl AccountFetcherFeeds for RpcAccountFetcher {
    async fn feeds_fetch_raw_account(&self, address: &Pubkey) -> anyhow::Result<AccountSharedData> {
        let sdfs: Result<AccountSharedData, anyhow::Error> = self.rpc
            .get_account_with_commitment(address, self.rpc.commitment())
            .await
            .with_context(|| format!("fetch account {}", *address))?
            .value
            // .ok_or(anyhow::ClientError::AccountNotFound)
            .ok_or(anyhow!("Account not found")) // TODO is this correct?
            .with_context(|| format!("fetch account {}", *address))
            .map(Into::into);
        sdfs
    }

    async fn feeds_fetch_program_accounts(
        &self,
        program: &Pubkey,
        discriminator: [u8; 8],
    ) -> anyhow::Result<Vec<(Pubkey, AccountSharedData)>> {
        use solana_account_decoder::UiAccountEncoding;
        use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
        use solana_client::rpc_filter::{Memcmp, RpcFilterType};
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                0,
                discriminator.to_vec(),
            ))]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(self.rpc.commitment()),
                ..RpcAccountInfoConfig::default()
            },
            with_context: Some(true),
        };
        let accs = self
            .rpc
            .get_program_accounts_with_config(program, config)
            .await?;
        // convert Account -> AccountSharedData
        Ok(accs
            .into_iter()
            .map(|(pk, acc)| (pk, acc.into()))
            .collect::<Vec<_>>())
    }
}
