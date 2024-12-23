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
use prometheus::{
    proto, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts,
};
use rdkafka::Statistics;
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub struct KafkaMetricsCollector {
    stats: Arc<RwLock<Statistics>>,
    descs: Vec<Desc>,

    // Core client metrics
    msg_cnt: IntGauge,
    msg_size: IntGauge,
    msg_max: IntGauge,
    msg_size_max: IntGauge,
    metadata_cache_cnt: IntGauge,
    tx: IntCounter,
    tx_bytes: IntCounter,
    rx: IntCounter,
    rx_bytes: IntCounter,
    txmsgs: IntCounter,
    txmsg_bytes: IntCounter,
    rxmsgs: IntCounter,
    rxmsg_bytes: IntCounter,

    // Broker metrics
    broker_state_cnt: IntGauge,
    broker_outbuf_cnt: IntGauge,
    broker_outbuf_msg_cnt: IntGauge,
    broker_waitresp_cnt: IntGauge,
    broker_waitresp_msg_cnt: IntGauge,
    broker_tx: IntCounter,
    broker_tx_bytes: IntCounter,
    broker_tx_errs: IntCounter,
    broker_tx_retries: IntCounter,
    broker_req_timeouts: IntCounter,
    broker_rx: IntCounter,
    broker_rx_bytes: IntCounter,
    broker_rx_errs: IntCounter,
    broker_rx_corrid_errs: IntCounter,
    broker_rx_partial: IntCounter,
    broker_connects: IntCounter,
    broker_disconnects: IntCounter,
    broker_int_latency: Histogram,
    broker_outbuf_latency: Histogram,
    broker_rtt: Histogram,
    broker_throttle: Histogram,

    // Topic metrics
    topic_metadata_age: IntGaugeVec,
    topic_batchsize: HistogramVec,
    topic_batchcnt: HistogramVec,

    // Partition metrics with labels
    partition_msgq_cnt: IntGaugeVec,
    partition_msgq_bytes: IntGaugeVec,
    partition_xmit_msgq_cnt: IntGaugeVec,
    partition_xmit_msgq_bytes: IntGaugeVec,
    partition_fetchq_cnt: IntGaugeVec,
    partition_fetchq_size: IntGaugeVec,
    partition_query_offset: IntGaugeVec,
    partition_next_offset: IntGaugeVec,
    partition_app_offset: IntGaugeVec,
    partition_stored_offset: IntGaugeVec,
    partition_committed_offset: IntGaugeVec,
    partition_eof_offset: IntGaugeVec,
    partition_lo_offset: IntGaugeVec,
    partition_hi_offset: IntGaugeVec,
    partition_consumer_lag: IntGaugeVec,
    partition_consumer_lag_stored: IntGaugeVec,
    partition_txmsgs: IntCounterVec,
    partition_txbytes: IntCounterVec,
    partition_rxmsgs: IntCounterVec,
    partition_rxbytes: IntCounterVec,
    partition_msgs: IntCounterVec,
    partition_rx_ver_drops: IntCounterVec,
    partition_msgs_inflight: IntGaugeVec,

    // Consumer group metrics
    cgrp_rebalance_cnt: IntCounter,
    cgrp_rebalance_age: IntGauge,
    cgrp_assignment_size: IntGauge,

    // Exactly once semantics metrics
    eos_epoch_cnt: IntCounter,
    eos_producer_id: IntGauge,
    eos_producer_epoch: IntGauge,
}

impl KafkaMetricsCollector {
    pub fn new(stats: Arc<RwLock<Statistics>>) -> anyhow::Result<KafkaMetricsCollector> {
        let mut descs = Vec::new();

        fn create_gauge_vec(
            name: &str,
            help: &str,
            labels: &[&str],
            descs: &mut Vec<Desc>,
        ) -> IntGaugeVec {
            let gauge = IntGaugeVec::new(Opts::new(name, help), labels).unwrap();
            descs.extend(gauge.clone().desc().into_iter().cloned());
            gauge
        }

        fn create_counter_vec(
            name: &str,
            help: &str,
            labels: &[&str],
            descs: &mut Vec<Desc>,
        ) -> IntCounterVec {
            let counter = IntCounterVec::new(Opts::new(name, help), labels).unwrap();
            descs.extend(counter.clone().desc().into_iter().cloned());
            counter
        }

        fn create_histogram_vec(
            name: &str,
            help: &str,
            labels: &[&str],
            descs: &mut Vec<Desc>,
        ) -> HistogramVec {
            let histogram = HistogramVec::new(HistogramOpts::new(name, help), labels).unwrap();
            descs.extend(histogram.clone().desc().into_iter().cloned());
            histogram
        }

        let topic_labels = &["topic"];
        let partition_labels = &["topic", "partition"];

        let collector = KafkaMetricsCollector {
            stats: stats.clone(),
            descs: descs.clone(),
            // Core metrics
            msg_cnt: IntGauge::new(
                "kafka_msg_cnt",
                "Current number of messages in producer queues",
            )?,
            msg_size: IntGauge::new(
                "kafka_msg_size",
                "Current total size of messages in producer queues",
            )?,
            msg_max: IntGauge::new(
                "kafka_msg_max",
                "Maximum number of messages allowed in producer queues",
            )?,
            msg_size_max: IntGauge::new(
                "kafka_msg_size_max",
                "Maximum total size of messages allowed in producer queues",
            )?,
            metadata_cache_cnt: IntGauge::new(
                "kafka_metadata_cache_cnt",
                "Number of topics in metadata cache",
            )?,
            tx: IntCounter::new("kafka_tx_total", "Total number of transmissions")?,
            tx_bytes: IntCounter::new("kafka_tx_bytes_total", "Total number of bytes transmitted")?,
            rx: IntCounter::new("kafka_rx_total", "Total number of receptions")?,
            rx_bytes: IntCounter::new("kafka_rx_bytes_total", "Total number of bytes received")?,
            txmsgs: IntCounter::new("kafka_txmsgs_total", "Total number of messages transmitted")?,
            txmsg_bytes: IntCounter::new(
                "kafka_txmsg_bytes_total",
                "Total number of message bytes transmitted",
            )?,
            rxmsgs: IntCounter::new("kafka_rxmsgs_total", "Total number of messages received")?,
            rxmsg_bytes: IntCounter::new(
                "kafka_rxmsg_bytes_total",
                "Total number of message bytes received",
            )?,

            // Broker metrics
            broker_state_cnt: IntGauge::new("kafka_broker_state", "Broker connection state")?,
            broker_outbuf_cnt: IntGauge::new(
                "kafka_broker_outbuf_cnt",
                "Number of requests awaiting transmission",
            )?,
            broker_outbuf_msg_cnt: IntGauge::new(
                "kafka_broker_outbuf_msg_cnt",
                "Number of messages awaiting transmission",
            )?,
            broker_waitresp_cnt: IntGauge::new(
                "kafka_broker_waitresp_cnt",
                "Number of requests in-flight",
            )?,
            broker_waitresp_msg_cnt: IntGauge::new(
                "kafka_broker_waitresp_msg_cnt",
                "Number of messages in-flight",
            )?,
            broker_tx: IntCounter::new("kafka_broker_tx_total", "Total broker transmissions")?,
            broker_tx_bytes: IntCounter::new(
                "kafka_broker_tx_bytes_total",
                "Total broker bytes transmitted",
            )?,
            broker_tx_errs: IntCounter::new(
                "kafka_broker_tx_errs_total",
                "Total broker transmission errors",
            )?,
            broker_tx_retries: IntCounter::new(
                "kafka_broker_tx_retries_total",
                "Total broker transmission retries",
            )?,
            broker_req_timeouts: IntCounter::new(
                "kafka_broker_req_timeouts_total",
                "Total broker request timeouts",
            )?,
            broker_rx: IntCounter::new("kafka_broker_rx_total", "Total broker receptions")?,
            broker_rx_bytes: IntCounter::new(
                "kafka_broker_rx_bytes_total",
                "Total broker bytes received",
            )?,
            broker_rx_errs: IntCounter::new(
                "kafka_broker_rx_errs_total",
                "Total broker reception errors",
            )?,
            broker_rx_corrid_errs: IntCounter::new(
                "kafka_broker_rx_corrid_errs_total",
                "Total broker correlation ID errors",
            )?,
            broker_rx_partial: IntCounter::new(
                "kafka_broker_rx_partial_total",
                "Total broker partial message sets",
            )?,
            broker_connects: IntCounter::new(
                "kafka_broker_connects_total",
                "Total broker connection attempts",
            )?,
            broker_disconnects: IntCounter::new(
                "kafka_broker_disconnects_total",
                "Total broker disconnections",
            )?,
            broker_int_latency: Histogram::with_opts(HistogramOpts::new(
                "kafka_broker_int_latency",
                "Internal broker latency",
            ))?,
            broker_outbuf_latency: Histogram::with_opts(HistogramOpts::new(
                "kafka_broker_outbuf_latency",
                "Outbuf latency",
            ))?,
            broker_rtt: Histogram::with_opts(HistogramOpts::new(
                "kafka_broker_rtt",
                "Broker round-trip time",
            ))?,

            broker_throttle: Histogram::with_opts(HistogramOpts::new(
                "kafka_broker_throttle",
                "Broker throttle time",
            ))?,
            // Topic metrics with labels
            topic_metadata_age: create_gauge_vec(
                "kafka_topic_metadata_age",
                "Age of topic metadata",
                topic_labels,
                &mut descs,
            ),
            topic_batchsize: create_histogram_vec(
                "kafka_topic_batchsize",
                "Topic batch sizes",
                topic_labels,
                &mut descs,
            ),
            topic_batchcnt: create_histogram_vec(
                "kafka_topic_batchcnt",
                "Topic batch counts",
                topic_labels,
                &mut descs,
            ),

            // Partition metrics with labels
            partition_msgq_cnt: create_gauge_vec(
                "kafka_partition_msgq_cnt",
                "Messages in partition queue",
                partition_labels,
                &mut descs,
            ),
            partition_msgq_bytes: create_gauge_vec(
                "kafka_partition_msgq_bytes",
                "Bytes in partition queue",
                partition_labels,
                &mut descs,
            ),
            partition_xmit_msgq_cnt: create_gauge_vec(
                "kafka_partition_xmit_msgq_cnt",
                "Messages in partition transmit queue",
                partition_labels,
                &mut descs,
            ),
            partition_xmit_msgq_bytes: create_gauge_vec(
                "kafka_partition_xmit_msgq_bytes",
                "Bytes in partition transmit queue",
                partition_labels,
                &mut descs,
            ),
            partition_fetchq_cnt: create_gauge_vec(
                "kafka_partition_fetchq_cnt",
                "Messages in partition fetch queue",
                partition_labels,
                &mut descs,
            ),
            partition_fetchq_size: create_gauge_vec(
                "kafka_partition_fetchq_size",
                "Size of partition fetch queue",
                partition_labels,
                &mut descs,
            ),
            partition_query_offset: create_gauge_vec(
                "kafka_partition_query_offset",
                "Current partition query offset",
                partition_labels,
                &mut descs,
            ),
            partition_next_offset: create_gauge_vec(
                "kafka_partition_next_offset",
                "Next partition offset",
                partition_labels,
                &mut descs,
            ),
            partition_app_offset: create_gauge_vec(
                "kafka_partition_app_offset",
                "Application partition offset",
                partition_labels,
                &mut descs,
            ),
            partition_stored_offset: create_gauge_vec(
                "kafka_partition_stored_offset",
                "Stored partition offset",
                partition_labels,
                &mut descs,
            ),
            partition_committed_offset: create_gauge_vec(
                "kafka_partition_committed_offset",
                "Committed partition offset",
                partition_labels,
                &mut descs,
            ),
            partition_eof_offset: create_gauge_vec(
                "kafka_partition_eof_offset",
                "EOF partition offset",
                partition_labels,
                &mut descs,
            ),
            partition_lo_offset: create_gauge_vec(
                "kafka_partition_lo_offset",
                "Low watermark partition offset",
                partition_labels,
                &mut descs,
            ),
            partition_hi_offset: create_gauge_vec(
                "kafka_partition_hi_offset",
                "High watermark partition offset",
                partition_labels,
                &mut descs,
            ),
            partition_consumer_lag: create_gauge_vec(
                "kafka_partition_consumer_lag",
                "Consumer lag",
                partition_labels,
                &mut descs,
            ),
            partition_consumer_lag_stored: create_gauge_vec(
                "kafka_partition_consumer_lag_stored",
                "Stored consumer lag",
                partition_labels,
                &mut descs,
            ),
            partition_txmsgs: create_counter_vec(
                "kafka_partition_txmsgs_total",
                "Total partition messages transmitted",
                partition_labels,
                &mut descs,
            ),
            partition_txbytes: create_counter_vec(
                "kafka_partition_txbytes_total",
                "Total partition bytes transmitted",
                partition_labels,
                &mut descs,
            ),
            partition_rxmsgs: create_counter_vec(
                "kafka_partition_rxmsgs_total",
                "Total partition messages received",
                partition_labels,
                &mut descs,
            ),
            partition_rxbytes: create_counter_vec(
                "kafka_partition_rxbytes_total",
                "Total partition bytes received",
                partition_labels,
                &mut descs,
            ),
            partition_msgs: create_counter_vec(
                "kafka_partition_msgs_total",
                "Total partition messages",
                partition_labels,
                &mut descs,
            ),
            partition_rx_ver_drops: create_counter_vec(
                "kafka_partition_rx_ver_drops_total",
                "Total partition version drops",
                partition_labels,
                &mut descs,
            ),
            partition_msgs_inflight: create_gauge_vec(
                "kafka_partition_msgs_inflight",
                "Messages in flight",
                partition_labels,
                &mut descs,
            ),
            cgrp_rebalance_cnt: IntCounter::new("kafka_cgrp_rebalance_total", "Total rebalances")?,
            cgrp_rebalance_age: IntGauge::new("kafka_cgrp_rebalance_age", "Rebalance age")?,
            cgrp_assignment_size: IntGauge::new("kafka_cgrp_assignment_size", "Assignment size")?,

            // Exactly once semantics metrics
            eos_epoch_cnt: IntCounter::new("kafka_eos_epoch_total", "Total number of epochs")?,
            eos_producer_id: IntGauge::new("kafka_eos_producer_id", "Producer ID")?,
            eos_producer_epoch: IntGauge::new("kafka_eos_producer_epoch", "Producer epoch")?,
        };

        let mut collector = collector;
        collector.descs = descs.clone();

        Ok(collector)
    }
}

impl Collector for KafkaMetricsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let stats = match self.stats.read() {
            Ok(stats) => stats,
            Err(_) => return vec![],
        };

        // Core metrics
        let mut mfs = Vec::new();
        self.msg_cnt.set(stats.msg_cnt as i64);
        self.msg_size.set(stats.msg_size as i64);
        self.msg_max.set(stats.msg_max as i64);
        self.msg_size_max.set(stats.msg_size_max as i64);
        self.metadata_cache_cnt.set(stats.metadata_cache_cnt);
        self.tx.inc_by(stats.tx as u64);
        self.tx_bytes.inc_by(stats.tx_bytes as u64);
        self.rx.inc_by(stats.rx as u64);
        self.rx_bytes.inc_by(stats.rx_bytes as u64);
        self.txmsgs.inc_by(stats.txmsgs as u64);
        self.txmsg_bytes.inc_by(stats.txmsg_bytes as u64);
        self.rxmsgs.inc_by(stats.rxmsgs as u64);
        self.rxmsg_bytes.inc_by(stats.rxmsg_bytes as u64);

        mfs.extend(self.msg_cnt.collect());
        mfs.extend(self.msg_size.collect());
        mfs.extend(self.msg_max.collect());
        mfs.extend(self.msg_size_max.collect());
        mfs.extend(self.metadata_cache_cnt.collect());
        mfs.extend(self.tx.collect());
        mfs.extend(self.tx_bytes.collect());
        mfs.extend(self.rx.collect());
        mfs.extend(self.rx_bytes.collect());
        mfs.extend(self.txmsgs.collect());
        mfs.extend(self.txmsg_bytes.collect());
        mfs.extend(self.rxmsgs.collect());
        mfs.extend(self.rxmsg_bytes.collect());

        // Broker metrics
        for (_broker_id, broker) in stats.brokers.iter() {
            self.broker_state_cnt.set(match broker.state.as_str() {
                "UP" => 1,
                "DOWN" => 0,
                _ => -1,
            });

            self.broker_outbuf_cnt.set(broker.outbuf_cnt);
            self.broker_outbuf_msg_cnt.set(broker.outbuf_msg_cnt);
            self.broker_waitresp_cnt.set(broker.waitresp_cnt);
            self.broker_waitresp_msg_cnt.set(broker.waitresp_msg_cnt);

            self.broker_tx.inc_by(broker.tx);
            self.broker_tx_bytes.inc_by(broker.txbytes);
            self.broker_tx_errs.inc_by(broker.txerrs);
            self.broker_tx_retries.inc_by(broker.txretries);
            self.broker_req_timeouts.inc_by(broker.req_timeouts);
            self.broker_rx.inc_by(broker.rx);
            self.broker_rx_bytes.inc_by(broker.rxbytes);
            self.broker_rx_errs.inc_by(broker.rxerrs);
            self.broker_rx_corrid_errs.inc_by(broker.rxcorriderrs);
            self.broker_rx_partial.inc_by(broker.rxpartial);

            if let Some(connects) = broker.connects {
                self.broker_connects.inc_by(connects as u64);
            }
            if let Some(disconnects) = broker.disconnects {
                self.broker_disconnects.inc_by(disconnects as u64);
            }

            // Latency metrics
            if let Some(ref latency) = broker.int_latency {
                self.broker_int_latency.observe(latency.avg as f64);
            }
            if let Some(ref latency) = broker.outbuf_latency {
                self.broker_outbuf_latency.observe(latency.avg as f64);
            }
            if let Some(ref rtt) = broker.rtt {
                self.broker_rtt.observe(rtt.avg as f64);
            }
            if let Some(ref throttle) = broker.throttle {
                self.broker_throttle.observe(throttle.avg as f64);
            }
        }

        mfs.extend(self.broker_state_cnt.collect());
        mfs.extend(self.broker_outbuf_cnt.collect());
        mfs.extend(self.broker_outbuf_msg_cnt.collect());
        mfs.extend(self.broker_waitresp_cnt.collect());
        mfs.extend(self.broker_waitresp_msg_cnt.collect());
        mfs.extend(self.broker_tx.collect());
        mfs.extend(self.broker_tx_bytes.collect());
        mfs.extend(self.broker_tx_errs.collect());
        mfs.extend(self.broker_tx_retries.collect());
        mfs.extend(self.broker_req_timeouts.collect());
        mfs.extend(self.broker_rx.collect());
        mfs.extend(self.broker_rx_bytes.collect());
        mfs.extend(self.broker_rx_errs.collect());
        mfs.extend(self.broker_rx_corrid_errs.collect());
        mfs.extend(self.broker_rx_partial.collect());
        mfs.extend(self.broker_connects.collect());
        mfs.extend(self.broker_disconnects.collect());
        mfs.extend(self.broker_int_latency.collect());
        mfs.extend(self.broker_outbuf_latency.collect());
        mfs.extend(self.broker_rtt.collect());
        mfs.extend(self.broker_throttle.collect());

        // Topic and partition metrics with labels
        for (topic_name, topic) in stats.topics.iter() {
            self.topic_metadata_age
                .with_label_values(&[topic_name])
                .set(topic.metadata_age);
            self.topic_batchsize
                .with_label_values(&[topic_name])
                .observe(topic.batchsize.avg as f64);
            self.topic_batchcnt
                .with_label_values(&[topic_name])
                .observe(topic.batchcnt.avg as f64);

            for (partition_id, partition) in topic.partitions.iter() {
                let labels = &[topic_name.as_str(), &partition_id.to_string()];
                self.partition_msgq_cnt
                    .with_label_values(labels)
                    .set(partition.msgq_cnt);
                self.partition_msgq_bytes
                    .with_label_values(labels)
                    .set(partition.msgq_bytes as i64);
                self.partition_xmit_msgq_cnt
                    .with_label_values(labels)
                    .set(partition.xmit_msgq_cnt);
                self.partition_xmit_msgq_bytes
                    .with_label_values(labels)
                    .set(partition.xmit_msgq_bytes as i64);
                self.partition_fetchq_cnt
                    .with_label_values(labels)
                    .set(partition.fetchq_cnt);
                self.partition_fetchq_size
                    .with_label_values(labels)
                    .set(partition.fetchq_size as i64);
                self.partition_query_offset
                    .with_label_values(labels)
                    .set(partition.query_offset);
                self.partition_next_offset
                    .with_label_values(labels)
                    .set(partition.next_offset);
                self.partition_app_offset
                    .with_label_values(labels)
                    .set(partition.app_offset);
                self.partition_stored_offset
                    .with_label_values(labels)
                    .set(partition.stored_offset);
                self.partition_committed_offset
                    .with_label_values(labels)
                    .set(partition.committed_offset);
                self.partition_eof_offset
                    .with_label_values(labels)
                    .set(partition.eof_offset);
                self.partition_lo_offset
                    .with_label_values(labels)
                    .set(partition.lo_offset);
                self.partition_hi_offset
                    .with_label_values(labels)
                    .set(partition.hi_offset);
                self.partition_consumer_lag
                    .with_label_values(labels)
                    .set(partition.consumer_lag);
                self.partition_consumer_lag_stored
                    .with_label_values(labels)
                    .set(partition.consumer_lag_stored);
                self.partition_txmsgs
                    .with_label_values(labels)
                    .inc_by(partition.txmsgs);
                self.partition_txbytes
                    .with_label_values(labels)
                    .inc_by(partition.txbytes);
                self.partition_rxmsgs
                    .with_label_values(labels)
                    .inc_by(partition.rxmsgs);
                self.partition_rxbytes
                    .with_label_values(labels)
                    .inc_by(partition.rxbytes);
                self.partition_msgs
                    .with_label_values(labels)
                    .inc_by(partition.msgs);
                self.partition_rx_ver_drops
                    .with_label_values(labels)
                    .inc_by(partition.rx_ver_drops);
                self.partition_msgs_inflight
                    .with_label_values(labels)
                    .set(partition.msgs_inflight);
            }
        }

        mfs.extend(self.topic_metadata_age.collect());
        mfs.extend(self.topic_batchsize.collect());
        mfs.extend(self.topic_batchcnt.collect());
        mfs.extend(self.partition_msgq_cnt.collect());
        mfs.extend(self.partition_msgq_bytes.collect());
        mfs.extend(self.partition_xmit_msgq_cnt.collect());
        mfs.extend(self.partition_xmit_msgq_bytes.collect());
        mfs.extend(self.partition_fetchq_cnt.collect());
        mfs.extend(self.partition_fetchq_size.collect());
        mfs.extend(self.partition_query_offset.collect());
        mfs.extend(self.partition_next_offset.collect());
        mfs.extend(self.partition_app_offset.collect());
        mfs.extend(self.partition_stored_offset.collect());
        mfs.extend(self.partition_committed_offset.collect());
        mfs.extend(self.partition_eof_offset.collect());
        mfs.extend(self.partition_lo_offset.collect());
        mfs.extend(self.partition_hi_offset.collect());
        mfs.extend(self.partition_consumer_lag.collect());
        mfs.extend(self.partition_consumer_lag_stored.collect());
        mfs.extend(self.partition_txmsgs.collect());
        mfs.extend(self.partition_txbytes.collect());
        mfs.extend(self.partition_rxmsgs.collect());
        mfs.extend(self.partition_rxbytes.collect());
        mfs.extend(self.partition_msgs.collect());
        mfs.extend(self.partition_rx_ver_drops.collect());
        mfs.extend(self.partition_msgs_inflight.collect());

        // Consumer group metrics
        if let Some(ref cgrp) = stats.cgrp {
            self.cgrp_rebalance_cnt.inc_by(cgrp.rebalance_cnt as u64);
            self.cgrp_rebalance_age.set(cgrp.rebalance_age);
            self.cgrp_assignment_size.set(cgrp.assignment_size as i64);
        }

        mfs.extend(self.cgrp_rebalance_cnt.collect());
        mfs.extend(self.cgrp_rebalance_age.collect());
        mfs.extend(self.cgrp_assignment_size.collect());

        // EOS metrics
        if let Some(ref eos) = stats.eos {
            self.eos_epoch_cnt.inc_by(eos.epoch_cnt as u64);
            self.eos_producer_id.set(eos.producer_id);
            self.eos_producer_epoch.set(eos.producer_epoch);
        }

        mfs.extend(self.eos_epoch_cnt.collect());
        mfs.extend(self.eos_producer_id.collect());
        mfs.extend(self.eos_producer_epoch.collect());

        mfs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    #[test]
    fn test_kafka_metrics_collector() {
        let stats = Arc::new(RwLock::new(Statistics::default()));
        let collector = KafkaMetricsCollector::new(stats).unwrap();

        let descs = collector.desc();
        assert!(!descs.is_empty());

        let mfs = collector.collect();
        assert!(!mfs.is_empty());

        let registry = Registry::new();
        assert!(registry.register(Box::new(collector)).is_ok());
    }
}
