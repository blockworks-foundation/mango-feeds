use solana_sdk::clock::Slot;
use std::str::FromStr;
use log::info;
use {
    solana_sdk::account::{AccountSharedData, ReadableAccount},
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
};
use crate::chain_data::WhatToDo::*;

use crate::metrics::*;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SlotStatus {
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
    pub write_version: u64,
    pub account: AccountSharedData,
}

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
    accounts: HashMap<Pubkey, Vec<AccountData>>,
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
    pub fn update_slot(&mut self, new_slot: SlotData) {
        let new_processed_head = new_slot.slot > self.newest_processed_slot;
        if new_processed_head {
            self.newest_processed_slot = new_slot.slot;
        }

        let new_rooted_head =
            new_slot.slot > self.newest_rooted_slot && new_slot.status == SlotStatus::Rooted;
        if new_rooted_head {
            self.newest_rooted_slot = new_slot.slot;
        }

        // Use the highest slot that has a known parent as best chain
        // (sometimes slots OptimisticallyConfirm before we even know the parent!)
        let new_best_chain = new_slot.parent.is_some() && new_slot.slot > self.best_chain_slot;
        if new_best_chain {
            self.best_chain_slot = new_slot.slot;
        }

        let mut parent_update = false;

        use std::collections::hash_map::Entry;
        match self.slots.entry(new_slot.slot) {
            Entry::Vacant(v) => {
                v.insert(new_slot);
            }
            Entry::Occupied(o) => {
                let v = o.into_mut();
                parent_update = v.parent != new_slot.parent && new_slot.parent.is_some();
                v.parent = v.parent.or(new_slot.parent);
                // Never decrease the slot status
                if v.status == SlotStatus::Processed || new_slot.status == SlotStatus::Rooted {
                    v.status = new_slot.status;
                }
            }
        };

        if new_best_chain || parent_update {
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
                writes.retain(|w| {
                    w.slot == newest_rooted_write_slot || w.slot > self.newest_rooted_slot
                });
                self.account_versions_stored += writes.len();
                self.account_bytes_stored +=
                    writes.iter().map(|w| w.account.data().len()).sum::<usize>()
            }

            // now it's fine to drop any slots before the new rooted head
            // as account writes for non-rooted slots before it have been dropped
            self.slots.retain(|s, _| *s >= self.newest_rooted_slot);
        }
    }

    pub fn update_account(&mut self, pubkey: Pubkey, account: AccountData) {
        use std::collections::hash_map::Entry;
        match self.accounts.entry(pubkey) {
            Entry::Vacant(v) => {
                println!("update_account_vacant: slot={}, write_version={}", account.slot, account.write_version);
                self.account_versions_stored += 1;
                self.account_bytes_stored += account.account.data().len();
                v.insert(vec![account]);
            }
            Entry::Occupied(o) => {
                println!("update_account_occupied: slot={}, write_version={}", account.slot, account.write_version);
                let v = o.into_mut();

                for el in v.iter() {
                    println!("> el.slot={}, el.write_version={}", el.slot, el.write_version);
                }

                // v is ordered by slot ascending. find the right position
                // overwrite if an entry for the slot already exists, otherwise insert
                let rev_pos = v
                    .iter()
                    .rev()
                    .position(|d| d.slot <= account.slot)
                    .unwrap_or(v.len());
                println!("rev_pos={}", rev_pos);
                let pos = v.len() - rev_pos;
                if pos < v.len() && v[pos].slot == account.slot {
                    println!("now check write version {} <= {}", v[pos].write_version, account.write_version);
                    if v[pos].write_version <= account.write_version {
                        v[pos] = account;
                    }
                } else {
                    println!("insert it");
                    self.account_versions_stored += 1;
                    self.account_bytes_stored += account.account.data().len();
                    v.insert(pos, account);
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

    pub fn newest_rooted_slot(&self) -> u64 {
        self.newest_rooted_slot
    }

    pub fn newest_processed_slot(&self) -> u64 {
        self.newest_processed_slot
    }

    pub fn raw_account_data(&self) -> &HashMap<Pubkey, Vec<AccountData>> {
        &self.accounts
    }

    pub fn raw_slot_data(&self) -> &HashMap<u64, SlotData> {
        &self.slots
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

#[derive(Debug, PartialEq)]
enum WhatToDo {
    Overwrite(usize),
    Insert(usize), // insert at position; move all elements to the right
    DoNothing,
}


#[test]
fn magic_overwrite_newer_write_version() {
    // v is ordered by slot ascending. find the right position
    // overwrite if an entry for the slot already exists, otherwise insert
    let fake_account_data = AccountSharedData::new(99999999, 999999, &Pubkey::new_unique());
    let mut v = given_v1235(fake_account_data);

    assert_eq!(the_logic(&mut v, 2, 1001), Overwrite(1));
}

#[test]
fn magic_overwrite_older_write_version() {
    // v is ordered by slot ascending. find the right position
    // overwrite if an entry for the slot already exists, otherwise insert
    let fake_account_data = AccountSharedData::new(99999999, 999999, &Pubkey::new_unique());
    let mut v = given_v1235(fake_account_data);

    assert_eq!(the_logic(&mut v, 2, 999), DoNothing);
}


#[test]
fn magic_insert_hole() {
    // v is ordered by slot ascending. find the right position
    // overwrite if an entry for the slot already exists, otherwise insert
    let fake_account_data = AccountSharedData::new(99999999, 999999, &Pubkey::new_unique());
    let mut v = given_v1235(fake_account_data);

    assert_eq!(the_logic(&mut v, 4, 1000), Insert(3));

}


#[test]
fn magic_min_max() {
    // v is ordered by slot ascending. find the right position
    // overwrite if an entry for the slot already exists, otherwise insert
    let fake_account_data = AccountSharedData::new(99999999, 999999, &Pubkey::new_unique());
    let mut v = given_v1235(fake_account_data);

    assert_eq!(the_logic(&mut v, 0, 1000), Insert(0)); // OK
    assert_eq!(the_logic(&mut v, 99, 1000), Insert(4));

}


#[test]
fn magic_append() {
    // v is ordered by slot ascending. find the right position
    // overwrite if an entry for the slot already exists, otherwise insert
    let fake_account_data = AccountSharedData::new(99999999, 999999, &Pubkey::new_unique());
    let mut v = given_v1235(fake_account_data);

    assert_eq!(the_logic(&mut v, 7, 1000), Insert(4));

}

fn given_v1235(fake_account_data: AccountSharedData) -> Vec<AccountData> {
    vec![
        AccountData {
            slot: 1,
            write_version: 1000,
            account: fake_account_data.clone(),
        },
        AccountData {
            slot: 2,
            write_version: 1000,
            account: fake_account_data.clone(),
        },
        AccountData {
            slot: 3,
            write_version: 1000,
            account: fake_account_data.clone(),
        },
        // no 4
        AccountData {
            slot: 5,
            write_version: 1000,
            account: fake_account_data.clone(),
        },
    ]
}

fn the_logic(v: &mut Vec<AccountData>, update_slot: Slot, update_write_version: u64) -> WhatToDo {
    // v is ordered by slot ascending. find the right position
    // overwrite if an entry for the slot already exists, otherwise insert
    let rev_pos = v
        .iter()
        .rev()
        .position(|d| d.slot <= update_slot)
        .unwrap_or(v.len());
    println!("rev_pos={}", rev_pos);
    let pos = v.len() - rev_pos;
    if pos < v.len() && v[pos].slot == update_slot {
        println!("now check write version {} <= {}", v[pos].write_version, update_write_version);
        if v[pos].write_version <= update_write_version {
            // v[pos] = account;
            return Overwrite(pos);
        } else {
            return DoNothing;
        }
    } else {
        println!("insert it");
        // v.insert(pos, account);
        return Insert(pos);
    }
}