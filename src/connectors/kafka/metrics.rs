use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use rdkafka::Statistics;
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub struct KafkaConsumerMetricsCollector {
    stats: Arc<RwLock<Statistics>>,
}

impl KafkaConsumerMetricsCollector {
    pub fn new(stats: Arc<RwLock<Statistics>>) -> Self {
        Self { stats }
    }

    pub fn statistics(&self) -> Result<Statistics, String> {
        match self.stats.read() {
            Ok(stats) => Ok(stats.clone()),
            Err(err) => Err(format!(
                "Cannot get kafka statistics from RwLock. Error: {}",
                err
            )),
        }
    }
}

impl Collector for KafkaConsumerMetricsCollector {
    fn desc(&self) -> Vec<&Desc> {
        //TODO:
        vec![]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        //TODO: encode metrics
        vec![]
    }
}
