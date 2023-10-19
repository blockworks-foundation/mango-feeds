// TODO move to feeds

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use async_once_cell::unpin::Lazy;

use anyhow::{Context, Error};


use solana_client::nonblocking::rpc_client::RpcClient as RpcClientAsync;
use solana_sdk::account::{AccountSharedData, ReadableAccount};
use solana_sdk::pubkey::Pubkey;


#[async_trait::async_trait]
pub trait AccountFetcherFeeds: Sync + Send {
    async fn feeds_fetch_raw_account(&self, address: &Pubkey) -> anyhow::Result<AccountSharedData>;
    async fn feeds_fetch_program_accounts(
        &self,
        program: &Pubkey,
        discriminator: [u8; 8],
    ) -> anyhow::Result<Vec<(Pubkey, AccountSharedData)>>;
}


pub struct RpcAccountFetcher {
    pub rpc: RpcClientAsync,
}


struct CoalescedAsyncJob<Key, Output> {
    jobs: HashMap<Key, Arc<Lazy<Output>>>,
}

impl<Key, Output> Default for CoalescedAsyncJob<Key, Output> {
    fn default() -> Self {
        Self {
            jobs: Default::default(),
        }
    }
}

impl<Key: std::cmp::Eq + std::hash::Hash, Output: 'static> CoalescedAsyncJob<Key, Output> {
    /// Either returns the job for `key` or registers a new job for it
    fn run_coalesced<F: std::future::Future<Output = Output> + Send + 'static>(
        &mut self,
        key: Key,
        fut: F,
    ) -> Arc<Lazy<Output>> {
        self.jobs
            .entry(key)
            .or_insert_with(|| Arc::new(Lazy::new(Box::pin(fut))))
            .clone()
    }

    fn remove(&mut self, key: &Key) {
        self.jobs.remove(key);
    }
}

#[derive(Default)]
struct AccountCache {
    accounts: HashMap<Pubkey, AccountSharedData>,
    keys_for_program_and_discriminator: HashMap<(Pubkey, [u8; 8]), Vec<Pubkey>>,

    account_jobs: CoalescedAsyncJob<Pubkey, anyhow::Result<AccountSharedData>>,
    program_accounts_jobs:
    CoalescedAsyncJob<(Pubkey, [u8; 8]), anyhow::Result<Vec<(Pubkey, AccountSharedData)>>>,
}

impl AccountCache {
    fn clear(&mut self) {
        self.accounts.clear();
        self.keys_for_program_and_discriminator.clear();
    }
}

pub struct CachedAccountFetcher<T: AccountFetcherFeeds> {
    fetcher: Arc<T>,
    cache: Arc<Mutex<AccountCache>>,
}

impl<T: AccountFetcherFeeds> Clone for CachedAccountFetcher<T> {
    fn clone(&self) -> Self {
        Self {
            fetcher: self.fetcher.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<T: AccountFetcherFeeds> CachedAccountFetcher<T> {
    pub fn new(fetcher: Arc<T>) -> Self {
        Self {
            fetcher,
            cache: Arc::new(Mutex::new(AccountCache::default())),
        }
    }

    pub fn clear_cache(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
    }
}

#[async_trait::async_trait]
impl<T: AccountFetcherFeeds + 'static> AccountFetcherFeeds for CachedAccountFetcher<T> {
    async fn feeds_fetch_raw_account(&self, address: &Pubkey) -> anyhow::Result<AccountSharedData> {
        let fetch_job = {
            let mut cache = self.cache.lock().unwrap();
            if let Some(acc) = cache.accounts.get(address) {
                return Ok(acc.clone());
            }

            // Start or fetch a reference to the fetch + cache update job
            let self_copy = self.clone();
            let address_copy = address.clone();
            cache.account_jobs.run_coalesced(*address, async move {
                let result = self_copy.fetcher.feeds_fetch_raw_account(&address_copy).await;
                let mut cache = self_copy.cache.lock().unwrap();

                // remove the job from the job list, so it can be redone if it errored
                cache.account_jobs.remove(&address_copy);

                // store a successful fetch
                if let Ok(account) = result.as_ref() {
                    cache.accounts.insert(address_copy, account.clone());
                }
                result
            })
        };

        match fetch_job.get().await {
            Ok(v) => Ok(v.clone()),
            // Can't clone the stored error, so need to stringize it
            Err(err) => Err(anyhow::format_err!(
                "fetch error in CachedAccountFetcher: {:?}",
                err
            )),
        }
    }

    async fn feeds_fetch_program_accounts(
        &self,
        program: &Pubkey,
        discriminator: [u8; 8],
    ) -> anyhow::Result<Vec<(Pubkey, AccountSharedData)>> {
        let cache_key = (*program, discriminator);
        let fetch_job = {
            let mut cache = self.cache.lock().unwrap();
            if let Some(accounts) = cache.keys_for_program_and_discriminator.get(&cache_key) {
                return Ok(accounts
                    .iter()
                    .map(|pk| (*pk, cache.accounts.get(&pk).unwrap().clone()))
                    .collect::<Vec<_>>());
            }

            let self_copy = self.clone();
            let program_copy = program.clone();
            cache
                .program_accounts_jobs
                .run_coalesced(cache_key.clone(), async move {
                    let result = self_copy
                        .fetcher
                        .feeds_fetch_program_accounts(&program_copy, discriminator)
                        .await;
                    let mut cache = self_copy.cache.lock().unwrap();
                    cache.program_accounts_jobs.remove(&cache_key);
                    if let Ok(accounts) = result.as_ref() {
                        cache
                            .keys_for_program_and_discriminator
                            .insert(cache_key, accounts.iter().map(|(pk, _)| *pk).collect());
                        for (pk, acc) in accounts.iter() {
                            cache.accounts.insert(*pk, acc.clone());
                        }
                    }
                    result
                })
        };

        match fetch_job.get().await {
            Ok(v) => Ok(v.clone()),
            // Can't clone the stored error, so need to stringize it
            Err(err) => Err(anyhow::format_err!(
                "fetch error in CachedAccountFetcher: {:?}",
                err
            )),
        }
    }
}

