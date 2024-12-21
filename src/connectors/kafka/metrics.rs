/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

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
