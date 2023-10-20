
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use mango_feeds_connector::{account_fetcher, chain_data};
use solana_client::nonblocking::rpc_client::{RpcClient as RpcClientAsync, RpcClient};
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_sdk::account::{AccountSharedData, ReadableAccount};
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake;
use mango_feeds_connector::account_fetcher::AccountFetcherFeeds;
use mango_feeds_connector::account_fetchers::RpcAccountFetcher;
use mango_feeds_connector::feeds_chain_data_fetcher::FeedsAccountFetcher;


#[tokio::main]
async fn main() {
    // let rpc_url: String = "https://api.mainnet-beta.solana.com/".to_string();
    let rpc_url: String = "https://api.devnet.solana.com/".to_string();

    program_accounts_rpc(&rpc_url).await;

    program_account_fetcher(rpc_url.clone()).await;

    account_fetcher(rpc_url.clone()).await;

}

/// get program accounts for Swap program and print the discriminators
/// 4636 accounts on devnet
async fn program_accounts_rpc(rpc_url: &String) {
    let rpc_client = RpcClientAsync::new(rpc_url.clone());

    let program_accounts_config = RpcProgramAccountsConfig {
        account_config: RpcAccountInfoConfig {
            encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
            ..RpcAccountInfoConfig::default()
        },
        ..RpcProgramAccountsConfig::default()
    };

    let program = Pubkey::from_str("SwaPpA9LAaLfeLi3a68M4DjnLqgtticKg6CnyNwgAC8").unwrap();

    let accounts = rpc_client
        .get_program_accounts_with_config(&program, program_accounts_config).await.unwrap();
    println!("num_of_accounts(unfiltered): {}", accounts.len());
    for (_pk, acc) in accounts {
        let discriminator = &acc.data()[..8];
        let mut di = [0;8];
        di.clone_from_slice(discriminator);
        println!("discriminator {:02X?}", di);
    }
}

async fn account_fetcher(rpc_url: String) {
    let client = RpcClientAsync::new(rpc_url);

    let account_fetcher = Arc::new(RpcAccountFetcher {
        rpc: client,
    });

    let account_key = Pubkey::from_str("2KgowxogBrGqRcgXQEmqFvC3PGtCu66qERNJevYW8Ajh").unwrap();
    let (acc, slot) = account_fetcher.feeds_fetch_raw_account(&account_key)
        .await.expect("account must exist");
    println!("price: {:?} by slot {}", acc.lamports(), slot);
}



/// 318 accounts on devnet by discriminator
async fn program_account_fetcher(rpc_url: String) {
    let client = RpcClientAsync::new(rpc_url);

    let account_fetcher = Arc::new(RpcAccountFetcher {
        rpc: client,
    });

    let program_key = Pubkey::from_str("SwaPpA9LAaLfeLi3a68M4DjnLqgtticKg6CnyNwgAC8").unwrap();

    // discriminator [01, 01, FC, 06, DD, F6, E1, D7]
    let discriminator =  [0x01, 0x01, 0xFC, 0x06, 0xDD, 0xF6, 0xE1, 0xD7];

    let (acc, slot) = account_fetcher.feeds_fetch_program_accounts(&program_key, discriminator)
        .await.expect("program account must exist");

    println!("program has {} accounts for discriminator {:02X?}", acc.len(), discriminator);
    println!("slot was {}", slot);
}
