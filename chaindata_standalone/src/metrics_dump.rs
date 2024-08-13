use std::f64::NAN;
use std::time::Duration;
use log::{debug, info, trace};
use prometheus::core::{Collector, Metric};
use prometheus::IntCounter;
use tokio::time::{Instant, Interval, interval, sleep};

pub fn start_metrics_dumper(prom_counter: &IntCounter) {
    let metric = prom_counter.clone();
    let name = prom_counter.desc().get(0).map(|x| x.fq_name.clone()).unwrap_or("noname".to_string());

    tokio::spawn(async move {

        let mut interval = interval(Duration::from_millis(3000));

        let mut last_observed_at = Instant::now();
        let mut last_observed_value: u64 = u64::MIN;

        loop {
            let value = metric.get();
            trace!("counter <{}> value: {}", name, value);

            if last_observed_value != u64::MIN {
                let elapsed = last_observed_at.elapsed().as_secs_f64();
                let delta = value - last_observed_value;
                debug!("counter <{}> (value={}) with throughput {:.1}/s", name, value, delta as f64 / elapsed);
            }

            last_observed_value = value;
            last_observed_at = Instant::now();

            interval.tick().await;
        }


    });
}