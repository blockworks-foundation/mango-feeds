use bytemuck::cast_ref;
use mango_v4::state::FillEvent;
use serum_dex::{instruction::MarketInstruction, state::EventView};
use solana_geyser_connector_lib::{
    account_write_filter::{self, AccountWriteRoute},
    chain_data::{AccountData, ChainData, SlotData},
    metrics::Metrics,
    serum::SerumEventQueueHeader,
    AccountWrite, SlotUpdate,
};

use anchor_lang::AccountDeserialize;
use log::*;
use solana_sdk::{
    account::{ReadableAccount, WritableAccount},
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    stake_history::Epoch,
};
use std::{
    borrow::BorrowMut,
    collections::{BTreeSet, HashMap, HashSet},
    convert::TryFrom,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{openbook_crank_sink::OpenbookCrankSink, mango_v4_perp_crank_sink::MangoV4PerpCrankSink};

const MAX_BACKLOG: usize = 2;
const TIMEOUT_INTERVAL: Duration = Duration::from_millis(400);

pub fn init(
    perp_queue_pks: Vec<(Pubkey, Pubkey)>,
    serum_queue_pks: Vec<(Pubkey, Pubkey)>,
    group_pk: Pubkey,
    metrics_sender: Metrics,
) -> anyhow::Result<(
    async_channel::Sender<AccountWrite>,
    async_channel::Sender<SlotUpdate>,
    async_channel::Receiver<Vec<Instruction>>,
)> {
    // Event queue updates can be consumed by client connections
    let (instruction_sender, instruction_receiver) = async_channel::unbounded::<Vec<Instruction>>();

    let routes = vec![
        AccountWriteRoute {
            matched_pubkeys: serum_queue_pks
                .iter()
                .map(|(_, evq_pk)| evq_pk.clone())
                .collect(),
            sink: Arc::new(OpenbookCrankSink::new(
                serum_queue_pks,
                instruction_sender.clone(),
            )),
            timeout_interval: Duration::default(),
        },
        AccountWriteRoute {
            matched_pubkeys: perp_queue_pks
                .iter()
                .map(|(_, evq_pk)| evq_pk.clone())
                .collect(),
            sink: Arc::new(MangoV4PerpCrankSink::new(
                perp_queue_pks,
                group_pk,
                instruction_sender.clone(),
            )),
            timeout_interval: Duration::default(),
        },
    ];

    let (account_write_queue_sender, slot_queue_sender) =
        account_write_filter::init(routes, metrics_sender.clone())?;

    Ok((
        account_write_queue_sender,
        slot_queue_sender,
        instruction_receiver,
    ))
}
