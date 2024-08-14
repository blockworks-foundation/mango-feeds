use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Lines, Read};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use csv::ReaderBuilder;
use itertools::Itertools;
use log::{info, trace};
use solana_sdk::account::AccountSharedData;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::pubkey::Pubkey;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use clap::Parser;
use mango_feeds_connector::chain_data::{AccountData, ChainData, SlotData, SlotStatus};


// #[derive(Parser, Debug, Clone)]
// #[clap()]
// struct Cli {
//     #[clap(short, long)]
//     replay_file: Option<String>,
// }


const RAYDIUM_AMM_PUBKEY: &'static str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

pub fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();

    // let Cli { replay_file } = Cli::parse();


    let mut chain_data = ChainData::new();
    let mut slot_cnt = 0;
    let mut account_cnt = 0;
    let started_at = Instant::now();
    // read line
    // let buffer: BufReader<_> = match replay_file {
    //     None => {
    //         info!("use data from inline csv");
    //         // 500k
    //         let data = include_bytes!("dump-slot-acccounts-fsn4-mixed-500k.csv");
    //         io::BufReader::new(data.as_ref())
    //     }
    //     Some(slot_stream_dump_file) => {
    //         info!("use replay_file: {}", slot_stream_dump_file);
    //         let file = File::open(slot_stream_dump_file).unwrap();
    //         io::BufReader::new(file)
    //     }
    // };

    let buffer = {
        info!("use data from inline csv");
        // 500k
        let data = include_bytes!("dump-slot-acccounts-fsn4-mixed-500k.csv");
        io::BufReader::new(data.as_ref())
    };
    for line in buffer.lines().flatten() {
        // update_slot.slot, update_slot.parent.unwrap_or(0), short_status, since_epoch_ms
        // slot, account_pk, write_version, data_len, since_epoch_ms
        let rows = line.split(",").collect_vec();

        if rows[0] == "MIXSLOT" {
            let slot: u64 = rows[1].parse().unwrap();
            let parent: Option<u64> = rows[2].parse().ok().and_then(|v| if v == 0 { None } else { Some(v) });
            let commitment_level = match rows[3].to_string().as_str() {
                "P" => CommitmentLevel::Processed,
                "C" => CommitmentLevel::Confirmed,
                "F" => CommitmentLevel::Finalized,
                _ => panic!("invalid commitment level"),
            };
            let since_epoch_ms: u64 = rows[4].trim().parse().unwrap();

            let slot_status = match commitment_level {
                CommitmentLevel::Processed => SlotStatus::Processed,
                CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                CommitmentLevel::Finalized => SlotStatus::Rooted,
                _ => panic!("invalid commitment level"),
            };
            const INIT_CHAIN: Slot = 0;
            trace!("MIXSLOT slot: {}, parent: {:?}, status: {:?}, since_epoch_ms: {}", slot, parent, slot_status, since_epoch_ms);
            let slot_data = SlotData {
                slot,
                parent,
                status: slot_status,
                chain: INIT_CHAIN,
            };
            slot_cnt += 1;
            chain_data.update_slot(slot_data);

        } else if rows[0] == "MIXACCOUNT" {
            let slot: u64 = rows[1].parse().unwrap();
            let account_pk: String = rows[2].parse().unwrap();
            let account_pk = Pubkey::from_str(&account_pk).unwrap();
            let write_version: u64 = rows[3].parse().unwrap();
            let data_len: u64 = rows[4].parse().unwrap();
            let since_epoch_ms: u64 = rows[5].trim().parse().unwrap();
            trace!("MIXACCOUNT slot: {}, account_pk: {}, write_version: {}, data_len: {}, since_epoch_ms: {}", slot, account_pk, write_version, data_len, since_epoch_ms);

            let account_data = AccountData {
                slot,
                write_version,
                account: AccountSharedData::new(slot, data_len as usize, &Pubkey::from_str(RAYDIUM_AMM_PUBKEY).unwrap())
            };

            account_cnt += 1;
            chain_data.update_account(account_pk, account_data);
        }

        if (slot_cnt + account_cnt) % 100_000 == 0 {
            info!("progress .. slot_cnt: {}, account_cnt: {}, elapsed: {:.02}s", slot_cnt, account_cnt, started_at.elapsed().as_secs_f64());
        }
    }

    info!("slot_cnt: {}, account_cnt: {}, elapsed: {:.02}s", slot_cnt, account_cnt, started_at.elapsed().as_secs_f64());

}
