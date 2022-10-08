use {
    log::*,
    serde_derive::{Deserialize, Serialize},
    solana_geyser_connector_lib::chain_data::ChainData,
    solana_geyser_connector_lib::*,
    solana_sdk::pubkey::Pubkey,
    std::str::FromStr,
    std::{
        fs::File,
        io::Read,
        sync::{Arc, RwLock},
    },
};

use fixed::types::I80F48;
use mango::state::{DataType, MangoAccount, MangoCache, MangoGroup, MAX_PAIRS};
use solana_geyser_connector_lib::metrics::*;
use solana_sdk::account::ReadableAccount;
use std::mem::size_of;

#[derive(Clone, Debug, Deserialize)]
pub struct PnlConfig {
    pub update_interval_millis: u64,
    pub mango_program: String,
    pub mango_group: String,
    pub mango_cache: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct JsonRpcConfig {
    pub bind_address: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub metrics: MetricsConfig,
    pub pnl: PnlConfig,
    pub jsonrpc_server: JsonRpcConfig,
}

type PnlData = Vec<(Pubkey, [I80F48; MAX_PAIRS])>;

fn compute_pnl(
    account: &MangoAccount,
    market_index: usize,
    group: &MangoGroup,
    cache: &MangoCache,
) -> I80F48 {
    let perp_account = &account.perp_accounts[market_index];
    let perp_market_cache = &cache.perp_market_cache[market_index];
    let price = cache.price_cache[market_index].price;
    let contract_size = group.perp_markets[market_index].base_lot_size;

    let base_pos = I80F48::from_num(perp_account.base_position * contract_size) * price;
    let quote_pos = perp_account.get_quote_position(&perp_market_cache);
    base_pos + quote_pos
}

// regularly updates pnl_data from chain_data
fn start_pnl_updater(
    config: PnlConfig,
    chain_data: Arc<RwLock<ChainData>>,
    pnl_data: Arc<RwLock<PnlData>>,
    metrics_pnls_tracked: MetricU64,
) {
    let program_pk = Pubkey::from_str(&config.mango_program).unwrap();
    let group_pk = Pubkey::from_str(&config.mango_group).unwrap();
    let cache_pk = Pubkey::from_str(&config.mango_cache).unwrap();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(
                config.update_interval_millis,
            ))
            .await;

            let snapshot = chain_data.read().unwrap().accounts_snapshot();

            // get the group and cache now
            let group = snapshot.get(&group_pk);
            let cache = snapshot.get(&cache_pk);
            if group.is_none() || cache.is_none() {
                continue;
            }
            let group: &MangoGroup = bytemuck::from_bytes(group.unwrap().account.data());
            let cache: &MangoCache = bytemuck::from_bytes(cache.unwrap().account.data());

            let mut pnls = Vec::with_capacity(snapshot.len());
            for (pubkey, account) in snapshot.iter() {
                let owner = account.account.owner();
                let data = account.account.data();
                if data.len() != size_of::<MangoAccount>()
                    || data[0] != DataType::MangoAccount as u8
                    || owner != &program_pk
                {
                    continue;
                }

                let mango_account: &MangoAccount = bytemuck::from_bytes(data);
                if mango_account.mango_group != group_pk {
                    continue;
                }

                let mut pnl_vals = [I80F48::ZERO; MAX_PAIRS];
                for market_index in 0..MAX_PAIRS {
                    pnl_vals[market_index] = compute_pnl(mango_account, market_index, group, cache);
                }

                // Alternatively, we could prepare the sorted and limited lists for each
                // market here. That would be faster and cause less contention on the pnl_data
                // lock, but it looks like it's very far from being an issue.
                pnls.push((pubkey.clone(), pnl_vals));
            }

            *pnl_data.write().unwrap() = pnls;
            metrics_pnls_tracked.clone().set(pnl_data.read().unwrap().len() as u64)
        }
    });
}

#[derive(Serialize, Deserialize, Debug)]
struct UnsettledPnlRankedRequest {
    market_index: u8,
    limit: u8,
    order: String,
}

#[derive(Serialize, Deserialize)]
struct PnlResponseItem {
    pnl: f64,
    pubkey: String,
}

use jsonrpsee::http_server::HttpServerHandle;
fn start_jsonrpc_server(
    config: JsonRpcConfig,
    pnl_data: Arc<RwLock<PnlData>>,
    metrics_reqs: MetricU64,
    metrics_invalid_reqs: MetricU64,
) -> anyhow::Result<HttpServerHandle> {
    use jsonrpsee::core::Error;
    use jsonrpsee::http_server::{HttpServerBuilder, RpcModule};
    use jsonrpsee::types::error::CallError;
    use std::net::SocketAddr;

    let server = HttpServerBuilder::default().build(config.bind_address.parse::<SocketAddr>()?)?;
    let mut module = RpcModule::new(());
    module.register_method("unsettledPnlRanked", move |params, _| {
        let req = params.parse::<UnsettledPnlRankedRequest>()?;
        metrics_reqs.clone().increment();
        let invalid =
            |s: &'static str| Err(Error::Call(CallError::InvalidParams(anyhow::anyhow!(s))));
        let limit = req.limit as usize;
        if limit > 20 {
            metrics_invalid_reqs.clone().increment();
            return invalid("'limit' must be <= 20");
        }
        let market_index = req.market_index as usize;
        if market_index >= MAX_PAIRS {
            metrics_invalid_reqs.clone().increment();
            return invalid("'market_index' must be < MAX_PAIRS");
        }
        if req.order != "ASC" && req.order != "DESC" {
            metrics_invalid_reqs.clone().increment();
            return invalid("'order' must be ASC or DESC");
        }

        // write lock, because we sort in-place...
        let mut pnls = pnl_data.write().unwrap();
        if req.order == "ASC" {
            pnls.sort_unstable_by(|a, b| a.1[market_index].cmp(&b.1[market_index]));
        } else {
            pnls.sort_unstable_by(|a, b| b.1[market_index].cmp(&a.1[market_index]));
        }
        let response = pnls
            .iter()
            .take(limit)
            .map(|p| PnlResponseItem {
                pnl: p.1[market_index].to_num::<f64>(),
                pubkey: p.0.to_string(),
            })
            .collect::<Vec<_>>();

        Ok(response)
    })?;

    Ok(server.start(module)?)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("requires a config file argument");
        return Ok(());
    }

    let config: Config = {
        let mut file = File::open(&args[1])?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        toml::from_str(&contents).unwrap()
    };

    solana_logger::setup_with_default("info");
    info!("startup");

    let metrics_tx = metrics::start(config.metrics, "pnl".into());

    let metrics_reqs =
        metrics_tx.register_u64("pnl_jsonrpc_reqs_total".into(), MetricType::Counter);
    let metrics_invalid_reqs =
        metrics_tx.register_u64("pnl_jsonrpc_reqs_invalid_total".into(), MetricType::Counter);   
    let metrics_pnls_tracked =
        metrics_tx.register_u64("pnl_num_tracked".into(), MetricType::Gauge);

    let chain_data = Arc::new(RwLock::new(ChainData::new()));
    let pnl_data = Arc::new(RwLock::new(PnlData::new()));

    start_pnl_updater(config.pnl.clone(), chain_data.clone(), pnl_data.clone(), metrics_pnls_tracked);

    // dropping the handle would exit the server
    let _http_server_handle = start_jsonrpc_server(config.jsonrpc_server.clone(), pnl_data, metrics_reqs, metrics_invalid_reqs)?;

    // start filling chain_data from the grpc plugin source
    let (account_write_queue_sender, slot_queue_sender) = memory_target::init(chain_data).await?;
    grpc_plugin_source::process_events(
        &config.source,
        account_write_queue_sender,
        slot_queue_sender,
        metrics_tx,
    )
    .await;

    Ok(())
}
