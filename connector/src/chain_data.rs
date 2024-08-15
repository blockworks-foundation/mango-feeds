use crate::chain_data::SlotVectorEffect::*;
use log::trace;
use smallvec::{smallvec, SmallVec};
use solana_sdk::clock::Slot;
use {
    solana_sdk::account::{AccountSharedData, ReadableAccount},
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
};

use crate::metrics::*;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SlotStatus {
    // aka Finalized
    Rooted,
    Confirmed,
    Processed,
}

#[derive(Clone, Debug)]
pub struct SlotData {
    pub slot: u64,
    pub parent: Option<u64>,
    pub status: SlotStatus,
    // the top slot that this is in a chain with. uncles will have values < tip
    // this field gets progressively rewritten as new successor slots are added
    pub chain: u64,
}

#[derive(Clone, Debug)]
pub struct AccountData {
    pub slot: u64,
    /// - caution: write_versions sourced from different Validator Node must not be compared
    /// - account data from gMA/gPA RPC "snapshot" do not contain write_version
    /// - from docs: "A single global atomic `AccountsDb::write_version` - tracks the number of commits to the entire data store."
    pub write_version: u64,
    pub account: AccountSharedData,
}

// caution: this is brittle if data comes from multiple sources
impl AccountData {
    pub fn is_newer_than(&self, slot: u64, write_version: u64) -> bool {
        (self.slot > slot) || (self.slot == slot && self.write_version > write_version)
    }
}

/// Track slots and account writes
///
/// - use account() to retrieve the current best data for an account.
/// - update_from_snapshot() and update_from_websocket() update the state for new messages
pub struct ChainData {
    /// only slots >= newest_rooted_slot are retained
    slots: HashMap<u64, SlotData>,
    /// writes to accounts, only the latest rooted write an newer are retained
    /// size distribution on startup: total:1105, size1:315, size2:146
    accounts: HashMap<Pubkey, SmallVec<[AccountData; 2]>>,
    newest_rooted_slot: u64,
    newest_processed_slot: u64,
    best_chain_slot: u64,
    account_versions_stored: usize,
    account_bytes_stored: usize,
}

impl ChainData {
    pub fn new() -> Self {
        Self {
            slots: HashMap::new(),
            accounts: HashMap::new(),
            newest_rooted_slot: 0,
            newest_processed_slot: 0,
            best_chain_slot: 0,
            account_versions_stored: 0,
            account_bytes_stored: 0,
        }
    }
}

impl Default for ChainData {
    fn default() -> Self {
        Self::new()
    }
}

impl ChainData {
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn update_slot(&mut self, new_slotdata: SlotData) {
        let SlotData {
            slot: new_slot,
            parent: new_parent,
            status: new_status,
            ..
        } = new_slotdata;

        trace!("update_slot from newslot {:?}", new_slot);
        let new_processed_head = new_slot > self.newest_processed_slot;
        if new_processed_head {
            self.newest_processed_slot = new_slot;
            trace!("use slot {} as newest_processed_slot", new_slot);
        }

        let new_rooted_head =
            new_slot > self.newest_rooted_slot && new_status == SlotStatus::Rooted;
        if new_rooted_head {
            self.newest_rooted_slot = new_slot;
            trace!("use slot {} as newest_rooted_slot", new_slot);
        }

        // Use the highest slot that has a known parent as best chain
        // (sometimes slots OptimisticallyConfirm before we even know the parent!)
        let new_best_chain = new_parent.is_some() && new_slot > self.best_chain_slot;
        if new_best_chain {
            self.best_chain_slot = new_slot;
            trace!("use slot {} as best_chain_slot", new_slot);
        }

        let mut parent_update = false;

        use std::collections::hash_map::Entry;
        match self.slots.entry(new_slot) {
            Entry::Vacant(v) => {
                v.insert(new_slotdata);
                trace!("inserted new slot {:?}", new_slot);
            }
            Entry::Occupied(o) => {
                let v = o.into_mut();
                parent_update = v.parent != new_parent && new_parent.is_some();
                if parent_update {
                    trace!(
                        "update parent of slot {}: {}->{}",
                        new_slot,
                        v.parent.unwrap_or(0),
                        new_parent.unwrap_or(0)
                    );
                }
                v.parent = v.parent.or(new_parent);
                // Never decrease the slot status
                if v.status == SlotStatus::Processed || new_status == SlotStatus::Rooted {
                    trace!(
                        "update status of slot {}: {:?}->{:?}",
                        new_slot,
                        v.status,
                        new_status
                    );
                    v.status = new_status;
                }
            }
        };

        if new_best_chain || parent_update {
            trace!("update chain data for slot {} and ancestors", new_slot);
            // update the "chain" field down to the first rooted slot
            let mut slot = self.best_chain_slot;
            loop {
                if let Some(data) = self.slots.get_mut(&slot) {
                    data.chain = self.best_chain_slot;
                    if data.status == SlotStatus::Rooted {
                        break;
                    }
                    if let Some(parent) = data.parent {
                        slot = parent;
                        continue;
                    }
                }
                break;
            }
        }

        if new_rooted_head {
            // for each account, preserve only writes > newest_rooted_slot, or the newest
            // rooted write
            self.account_versions_stored = 0;
            self.account_bytes_stored = 0;

            // TODO improve log
            trace!("update account data for slot {}", new_slot);
            self.clean_accounts_on_new_root();

            // now it's fine to drop any slots before the new rooted head
            // as account writes for non-rooted slots before it have been dropped
            self.slots.retain(|s, _| *s >= self.newest_rooted_slot);
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    fn clean_accounts_on_new_root(&mut self) {
        for (_, writes) in self.accounts.iter_mut() {
            let newest_rooted_write_slot = Self::newest_rooted_write(
                writes,
                self.newest_rooted_slot,
                self.best_chain_slot,
                &self.slots,
            )
            .map(|w| w.slot)
            // no rooted write found: produce no effect, since writes > newest_rooted_slot are retained anyway
            .unwrap_or(self.newest_rooted_slot + 1);
            writes
                .retain(|w| w.slot == newest_rooted_write_slot || w.slot > self.newest_rooted_slot);
            self.account_versions_stored += writes.len();
            self.account_bytes_stored +=
                writes.iter().map(|w| w.account.data().len()).sum::<usize>()
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub fn update_account(&mut self, pubkey: Pubkey, account: AccountData) {
        if account.write_version == 0 {
            // some upstream components provide write_version=0 for snapshot accounts from gMA/gPA
            // this maybe defies the intended effect that the snapshot account data should overwrite data from grpc, etc.
            // would recommend to provide a very high write_version instead
            // disclaimer(groovie, 2024/08): this logic is controversial, and I'm flexible to remove this completely
            trace!("account {} has write_version 0 - not recommended", pubkey);
        }

        use std::collections::hash_map::Entry;
        match self.accounts.entry(pubkey) {
            Entry::Vacant(v) => {
                self.account_versions_stored += 1;
                self.account_bytes_stored += account.account.data().len();
                v.insert(smallvec![account]);
            }
            Entry::Occupied(o) => {
                let v_effect =
                    update_slotvec_logic(o.get().as_slice(), account.slot, account.write_version);

                let v = o.into_mut();

                match v_effect {
                    Overwrite(pos) => {
                        v[pos] = account;
                    }
                    Prepend => {
                        self.account_versions_stored += 1;
                        self.account_bytes_stored += account.account.data().len();
                        v.insert(0, account);
                    }
                    InsertAfter(pos) => {
                        self.account_versions_stored += 1;
                        self.account_bytes_stored += account.account.data().len();
                        v.insert(pos + 1, account);
                    }
                    DoNothing => {}
                }
            }
        };
    }

    fn is_account_write_live(&self, write: &AccountData) -> bool {
        self.slots
            .get(&write.slot)
            // either the slot is rooted or in the current chain
            .map(|s| {
                s.status == SlotStatus::Rooted
                    || s.chain == self.best_chain_slot
                    || write.slot > self.best_chain_slot
            })
            // if the slot can't be found but preceeds newest rooted, use it too (old rooted slots are removed)
            .unwrap_or(write.slot <= self.newest_rooted_slot || write.slot > self.best_chain_slot)
    }

    // rooted=finalized
    fn newest_rooted_write<'a>(
        writes: &'a [AccountData],
        newest_rooted_slot: u64,
        best_chain_slot: u64,
        slots: &HashMap<u64, SlotData>,
    ) -> Option<&'a AccountData> {
        writes.iter().rev().find(|w| {
            w.slot <= newest_rooted_slot
                && slots
                    .get(&w.slot)
                    .map(|s| {
                        // sometimes we seem not to get notifications about slots
                        // getting rooted, hence assume non-uncle slots < newest_rooted_slot
                        // are rooted too
                        s.status == SlotStatus::Rooted || s.chain == best_chain_slot
                    })
                    // preserved account writes for deleted slots <= newest_rooted_slot
                    // are expected to be rooted
                    .unwrap_or(true)
        })
    }

    /// Cloned snapshot of all the most recent live writes per pubkey
    pub fn accounts_snapshot(&self) -> HashMap<Pubkey, AccountData> {
        self.iter_accounts()
            .map(|(pk, a)| (*pk, a.clone()))
            .collect()
    }

    /// Ref to the most recent live write of the pubkey
    pub fn account(&self, pubkey: &Pubkey) -> anyhow::Result<&AccountData> {
        self.accounts
            .get(pubkey)
            .ok_or_else(|| anyhow::anyhow!("account {} not found", pubkey))?
            .iter()
            .rev()
            .find(|w| self.is_account_write_live(w))
            .ok_or_else(|| anyhow::anyhow!("account {} has no live data", pubkey))
    }

    /// Iterate over the most recent live data for all stored accounts
    pub fn iter_accounts(&self) -> impl Iterator<Item = (&Pubkey, &AccountData)> {
        self.accounts.iter().filter_map(|(pk, writes)| {
            writes
                .iter()
                .rev()
                .find(|w| self.is_account_write_live(w))
                .map(|latest_write| (pk, latest_write))
        })
    }

    /// Iterate over the most recent rooted data for all stored accounts
    pub fn iter_accounts_rooted(&self) -> impl Iterator<Item = (&Pubkey, &AccountData)> {
        self.accounts.iter().filter_map(|(pk, writes)| {
            Self::newest_rooted_write(
                writes,
                self.newest_rooted_slot,
                self.best_chain_slot,
                &self.slots,
            )
            .map(|latest_write| (pk, latest_write))
        })
    }

    pub fn slots_count(&self) -> usize {
        self.slots.len()
    }

    pub fn accounts_count(&self) -> usize {
        self.accounts.len()
    }

    pub fn account_writes_count(&self) -> usize {
        self.account_versions_stored
    }

    pub fn account_bytes(&self) -> usize {
        self.account_bytes_stored
    }

    pub fn best_chain_slot(&self) -> u64 {
        self.best_chain_slot
    }

    // aka newest finalized
    pub fn newest_rooted_slot(&self) -> u64 {
        self.newest_rooted_slot
    }

    pub fn newest_processed_slot(&self) -> u64 {
        self.newest_processed_slot
    }

    pub fn raw_slot_data(&self) -> &HashMap<u64, SlotData> {
        &self.slots
    }
}

#[derive(Debug, PartialEq)]
pub enum SlotVectorEffect {
    Overwrite(usize),
    Prepend,
    InsertAfter(usize),
    DoNothing,
}

#[inline]
pub fn update_slotvec_logic(
    v: &[AccountData],
    update_slot: Slot,
    update_write_version: u64,
) -> SlotVectorEffect {
    // v is ordered by slot ascending. find the right position
    // overwrite if an entry for the slot already exists, otherwise insert
    let pos = v
        .iter()
        .rev()
        .position(|d| d.slot <= update_slot)
        .map(|rev_pos| v.len() - 1 - rev_pos);

    match pos {
        Some(pos) => {
            if v[pos].slot == update_slot {
                if v[pos].write_version <= update_write_version {
                    // note: applies last-wins-strategy if write_version is equal
                    Overwrite(pos)
                } else {
                    DoNothing
                }
            } else {
                assert!(v[pos].slot < update_slot);
                InsertAfter(pos)
            }
        }
        None => Prepend,
    }
}

pub struct ChainDataMetrics {
    slots_stored: MetricU64,
    accounts_stored: MetricU64,
    account_versions_stored: MetricU64,
    account_bytes_stored: MetricU64,
}

impl ChainDataMetrics {
    pub fn new(metrics: &Metrics) -> Self {
        Self {
            slots_stored: metrics.register_u64("chaindata_slots_stored".into(), MetricType::Gauge),
            accounts_stored: metrics
                .register_u64("chaindata_accounts_stored".into(), MetricType::Gauge),
            account_versions_stored: metrics.register_u64(
                "chaindata_account_versions_stored".into(),
                MetricType::Gauge,
            ),
            account_bytes_stored: metrics
                .register_u64("chaindata_account_bytes_stored".into(), MetricType::Gauge),
        }
    }

    pub fn report(&mut self, chain: &ChainData) {
        self.slots_stored.set(chain.slots_count() as u64);
        self.accounts_stored.set(chain.accounts_count() as u64);
        self.account_versions_stored
            .set(chain.account_writes_count() as u64);
        self.account_bytes_stored.set(chain.account_bytes() as u64);
    }

    pub fn spawn_report_job(
        chain: std::sync::Arc<std::sync::RwLock<ChainData>>,
        metrics: &Metrics,
        interval: std::time::Duration,
    ) {
        let mut m = Self::new(metrics);

        let mut interval = tokio::time::interval(interval);
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                let chain_lock = chain.read().unwrap();
                m.report(&chain_lock);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::chain_data::{update_slotvec_logic, SlotVectorEffect::*};
    use crate::chain_data::{AccountData, ChainData, SlotData, SlotStatus};
    use solana_sdk::account::{AccountSharedData, ReadableAccount};
    use solana_sdk::clock::Slot;
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;

    #[test]
    pub fn test_loosing_account_write() {
        let owner = Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap();
        let my_account = Pubkey::new_unique();
        let mut chain_data = ChainData::new();

        chain_data.update_account(
            my_account,
            AccountData {
                slot: 123,
                write_version: 1,
                account: AccountSharedData::new(100, 100 /*space*/, &owner),
            },
            j,
        );

        chain_data.update_slot(SlotData {
            slot: 123,
            parent: None,
            status: SlotStatus::Rooted, // =finalized
            chain: 0,
        });

        chain_data.update_account(
            my_account,
            AccountData {
                slot: 128,
                write_version: 1,
                account: AccountSharedData::new(101, 101 /*space*/, &owner),
            },
        );

        chain_data.update_slot(SlotData {
            slot: 128,
            parent: Some(123),
            status: SlotStatus::Processed,
            chain: 0,
        });

        assert_eq!(chain_data.newest_rooted_slot(), 123);
        assert_eq!(chain_data.best_chain_slot(), 128);
        assert_eq!(chain_data.account(&my_account).unwrap().slot, 128);

        chain_data.update_slot(SlotData {
            slot: 129,
            parent: Some(128),
            status: SlotStatus::Processed,
            chain: 0,
        });

        assert_eq!(chain_data.newest_rooted_slot(), 123);
        assert_eq!(chain_data.best_chain_slot(), 129);
        assert_eq!(chain_data.account(&my_account).unwrap().slot, 128);
    }

    #[test]
    pub fn test_move_slot_to_finalized() {
        const SLOT: Slot = 42_000_000;
        const SOME_LAMPORTS: u64 = 99000;

        let owner = Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap();
        let my_account = Pubkey::new_unique();
        let mut chain_data = ChainData::new();

        chain_data.update_account(
            my_account,
            AccountData {
                slot: SLOT,
                write_version: 2000,
                account: AccountSharedData::new(SOME_LAMPORTS, 100 /*space*/, &owner),
            },
        );

        // note: this is initial state
        assert_eq!(chain_data.newest_rooted_slot(), 0);

        // assume no rooted slot yet
        assert_eq!(chain_data.iter_accounts_rooted().count(), 0);

        chain_data.update_slot(SlotData {
            slot: SLOT,
            parent: None,
            status: SlotStatus::Processed,
            chain: 0,
        });

        assert_eq!(chain_data.newest_rooted_slot(), 0);

        chain_data.update_slot(SlotData {
            slot: SLOT - 2,
            parent: None,
            status: SlotStatus::Rooted, // =finalized
            chain: 0,
        });

        assert_eq!(chain_data.newest_rooted_slot(), SLOT - 2);

        assert_eq!(chain_data.iter_accounts_rooted().count(), 0);

        // GIVEN: finalized slot SLOT
        chain_data.update_slot(SlotData {
            slot: SLOT,
            parent: None,
            status: SlotStatus::Rooted, // =finalized
            chain: 0,
        });

        assert_eq!(chain_data.iter_accounts_rooted().count(), 1);
    }

    #[test]
    pub fn test_must_not_overwrite_with_older_by_slot() {
        const SLOT: Slot = 42_000_000;

        let owner = Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap();
        let my_account = Pubkey::new_unique();
        let mut chain_data = ChainData::new();

        chain_data.update_slot(SlotData {
            slot: SLOT - 2,
            parent: None,
            status: SlotStatus::Rooted, // =finalized
            chain: 0,
        });

        chain_data.update_account(
            my_account,
            AccountData {
                slot: SLOT - 2,
                write_version: 2000,
                account: AccountSharedData::new(300, 100 /*space*/, &owner),
            },
        );

        assert_eq!(chain_data.iter_accounts_rooted().count(), 1);
        assert_eq!(
            chain_data.account(&my_account).unwrap().account.lamports(),
            300
        );

        // WHEN: update with older data according to slot
        chain_data.update_account(
            my_account,
            AccountData {
                slot: SLOT - 20,
                write_version: 2000,
                account: AccountSharedData::new(350, 100 /*space*/, &owner),
            },
        );
        // THEN: should not overwrite
        assert_eq!(
            chain_data.account(&my_account).unwrap().account.lamports(),
            300,
            "should not overwrite if slot is older"
        );
    }

    #[test]
    pub fn test_overwrite_with_older_by_write_version() {
        const SLOT: Slot = 42_000_000;

        let owner = Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap();
        let my_account = Pubkey::new_unique();
        let mut chain_data = ChainData::new();

        chain_data.update_slot(SlotData {
            slot: SLOT - 2,
            parent: None,
            status: SlotStatus::Rooted, // =finalized
            chain: 0,
        });

        chain_data.update_account(
            my_account,
            AccountData {
                slot: SLOT - 2,
                write_version: 2000,
                account: AccountSharedData::new(300, 100 /*space*/, &owner),
            },
        );

        assert_eq!(chain_data.iter_accounts_rooted().count(), 1);
        assert_eq!(
            chain_data.account(&my_account).unwrap().account.lamports(),
            300
        );

        // WHEN: update with older data according to write_version
        chain_data.update_account(
            my_account,
            AccountData {
                slot: SLOT - 2,
                write_version: 1980,
                account: AccountSharedData::new(400, 100 /*space*/, &owner),
            },
        );
        assert_eq!(
            chain_data.account(&my_account).unwrap().account.lamports(),
            300,
            "should not overwrite if write_version is older"
        );
    }

    #[test]
    fn slotvec_overwrite_newer_write_version() {
        // v is ordered by slot ascending. find the right position
        // overwrite if an entry for the slot already exists, otherwise insert
        let dummy_account_data = AccountSharedData::new(99999999, 999999, &Pubkey::new_unique());
        // 10 - 20 - 30 - 50
        let v = given_v1235(dummy_account_data);

        assert_eq!(update_slotvec_logic(&v, 20, 20000), Overwrite(1));
    }

    #[test]
    fn slotvec_overwrite_older_write_version() {
        // v is ordered by slot ascending. find the right position
        // overwrite if an entry for the slot already exists, otherwise insert
        let dummy_account_data = AccountSharedData::new(99999999, 999999, &Pubkey::new_unique());
        // 10 - 20 - 30 - 50
        let v = given_v1235(dummy_account_data);

        assert_eq!(update_slotvec_logic(&v, 20, 999), DoNothing);
    }

    #[test]
    fn slotvec_insert_hole() {
        // v is ordered by slot ascending. find the right position
        // overwrite if an entry for the slot already exists, otherwise insert
        let dummy_account_data = AccountSharedData::new(99999999, 999999, &Pubkey::new_unique());
        // 10 - 20 - 30 - 50
        let v = given_v1235(dummy_account_data);

        assert_eq!(update_slotvec_logic(&v, 40, 10040), InsertAfter(2));
    }

    #[test]
    fn slotvec_insert_left() {
        // v is ordered by slot ascending. find the right position
        // overwrite if an entry for the slot already exists, otherwise insert
        let dummy_account_data = AccountSharedData::new(99999999, 999999, &Pubkey::new_unique());
        // 10 - 20 - 30 - 50
        let v = given_v1235(dummy_account_data);

        // insert before first slot (10)
        assert_eq!(update_slotvec_logic(&v, 5, 500), Prepend); // OK
    }

    // this should be the most common case
    #[test]
    fn slotvec_append() {
        // v is ordered by slot ascending. find the right position
        // overwrite if an entry for the slot already exists, otherwise insert
        let dummy_account_data = AccountSharedData::new(99999999, 999999, &Pubkey::new_unique());
        // 10 - 20 - 30 - 50
        let v = given_v1235(dummy_account_data);

        assert_eq!(update_slotvec_logic(&v, 90, 50000), InsertAfter(3));
    }

    // 10 - 20 - 30 - 50
    fn given_v1235(dummy_account_data: AccountSharedData) -> Vec<AccountData> {
        vec![
            AccountData {
                slot: 10,
                write_version: 10010,
                account: dummy_account_data.clone(),
            },
            AccountData {
                slot: 20,
                write_version: 10020,
                account: dummy_account_data.clone(),
            },
            AccountData {
                slot: 30,
                write_version: 10030,
                account: dummy_account_data.clone(),
            },
            // no 40
            AccountData {
                slot: 50,
                write_version: 10050,
                account: dummy_account_data.clone(),
            },
        ]
    }
}
