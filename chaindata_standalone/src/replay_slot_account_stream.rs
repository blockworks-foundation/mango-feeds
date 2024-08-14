use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::PathBuf;
use std::str::FromStr;
use csv::ReaderBuilder;
use itertools::Itertools;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::pubkey::Pubkey;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use mango_feeds_connector::chain_data::{ChainData, SlotData, SlotStatus};

pub fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let slot_stream_dump_file = PathBuf::from_str("/Users/stefan/mango/projects/mango-feeds-connector/dump-slot-acccounts-fsn4-mixed.csv").unwrap();

    // read lins
    let file = File::open(slot_stream_dump_file).unwrap();
    let lines = io::BufReader::new(file).lines();
    for line in lines.flatten() {
        // update_slot.slot, update_slot.parent.unwrap_or(0), short_status, since_epoch_ms
        // slot, account_pk, write_version, data_len, since_epoch_ms
        let rows = line.split(",").collect_vec();

        if rows[0] == "MIXSLOT" {
            let slot: u64 = rows[1].parse().unwrap();
            let parent: Option<u64> = rows[2].parse().ok().and_then(|v| if v == 0 { None } else { Some(v) });
            let status = match rows[3].to_string().as_str() {
                "P" => CommitmentLevel::Processed,
                "C" => CommitmentLevel::Confirmed,
                "F" => CommitmentLevel::Finalized,
                _ => panic!("invalid commitment level"),
            };
            let since_epoch_ms: u64 = rows[4].trim().parse().unwrap();

            let status = match status {
                CommitmentLevel::Processed => SlotStatus::Processed,
                CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                CommitmentLevel::Finalized => SlotStatus::Rooted,
                _ => panic!("invalid commitment level"),
            };
            const INIT_CHAIN: Slot = 0;
            println!("MIXSLOT slot: {}, parent: {:?}, status: {:?}, since_epoch_ms: {}", slot, parent, status, since_epoch_ms);
        } else if rows[0] == "MIXACCOUNT" {
            let slot: u64 = rows[1].parse().unwrap();
            let account_pk: String = rows[2].parse().unwrap();
            let account_pk = Pubkey::from_str(&account_pk).unwrap();
            let write_version: u64 = rows[3].parse().unwrap();
            let data_len: u64 = rows[4].parse().unwrap();
            let since_epoch_ms: u64 = rows[5].trim().parse().unwrap();
            println!("MIXACCOUNT slot: {}, account_pk: {}, write_version: {}, data_len: {}, since_epoch_ms: {}", slot, account_pk, write_version, data_len, since_epoch_ms);
        }
    }


}
