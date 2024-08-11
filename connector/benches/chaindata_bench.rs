use criterion::{criterion_group, criterion_main, Criterion};
use mango_feeds_connector::chain_data::{update_slotvec_logic, AccountData};
use solana_sdk::account::AccountSharedData;
use solana_sdk::pubkey::Pubkey;
use std::hint::black_box;

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

fn criterion_benchmark(c: &mut Criterion) {
    // overwrite if an entry for the slot already exists, otherwise insert
    let dummy_account_data = AccountSharedData::new(99999999, 999999, &Pubkey::new_unique());
    let v = given_v1235(dummy_account_data);

    c.bench_function("update_slotvec_logic", |b| {
        b.iter(|| update_slotvec_logic(black_box(&v), 40, 10040))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
