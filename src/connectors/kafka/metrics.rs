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
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    proto,
};
use rdkafka::Statistics;
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub struct KafkaMetricsCollector {
    stats: Arc<RwLock<Statistics>>,
    descs: Vec<Desc>,
    core_metrics: CoreMetrics,
    broker_metrics: BrokerMetrics,
    topic_metrics: TopicMetrics,
    partition_metrics: PartitionMetrics,
    consumer_metrics: ConsumerGroupMetrics,
    eos_metrics: EosMetrics,
}

#[derive(Debug)]
struct CoreMetrics {
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
}

#[derive(Debug)]
struct BrokerMetrics {
    state_cnt: IntGauge,
    outbuf_cnt: IntGauge,
    outbuf_msg_cnt: IntGauge,
    waitresp_cnt: IntGauge,
    waitresp_msg_cnt: IntGauge,
    tx: IntCounter,
    tx_bytes: IntCounter,
    tx_errs: IntCounter,
    tx_retries: IntCounter,
    req_timeouts: IntCounter,
    rx: IntCounter,
    rx_bytes: IntCounter,
    rx_errs: IntCounter,
    rx_corrid_errs: IntCounter,
    rx_partial: IntCounter,
    connects: IntCounter,
    disconnects: IntCounter,
    int_latency: Histogram,
    outbuf_latency: Histogram,
    rtt: Histogram,
    throttle: Histogram,
}

#[derive(Debug)]
struct TopicMetrics {
    metadata_age: IntGaugeVec,
    batchsize: HistogramVec,
    batchcnt: HistogramVec,
}

#[derive(Debug)]
struct PartitionMetrics {
    msgq_cnt: IntGaugeVec,
    msgq_bytes: IntGaugeVec,
    xmit_msgq_cnt: IntGaugeVec,
    xmit_msgq_bytes: IntGaugeVec,
    fetchq_cnt: IntGaugeVec,
    fetchq_size: IntGaugeVec,
    query_offset: IntGaugeVec,
    next_offset: IntGaugeVec,
    app_offset: IntGaugeVec,
    stored_offset: IntGaugeVec,
    committed_offset: IntGaugeVec,
    eof_offset: IntGaugeVec,
    lo_offset: IntGaugeVec,
    hi_offset: IntGaugeVec,
    consumer_lag: IntGaugeVec,
    consumer_lag_stored: IntGaugeVec,
    txmsgs: IntCounterVec,
    txbytes: IntCounterVec,
    rxmsgs: IntCounterVec,
    rxbytes: IntCounterVec,
    msgs: IntCounterVec,
    rx_ver_drops: IntCounterVec,
    msgs_inflight: IntGaugeVec,
}

#[derive(Debug)]
struct ConsumerGroupMetrics {
    rebalance_cnt: IntCounter,
    rebalance_age: IntGauge,
    assignment_size: IntGauge,
}

#[derive(Debug)]
struct EosMetrics {
    epoch_cnt: IntCounter,
    producer_id: IntGauge,
    producer_epoch: IntGauge,
}

impl CoreMetrics {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
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
        })
    }
}

impl BrokerMetrics {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            state_cnt: IntGauge::new("kafka_broker_state", "Broker connection state")?,
            outbuf_cnt: IntGauge::new(
                "kafka_broker_outbuf_cnt",
                "Number of requests awaiting transmission",
            )?,
            outbuf_msg_cnt: IntGauge::new(
                "kafka_broker_outbuf_msg_cnt",
                "Number of messages awaiting transmission",
            )?,
            waitresp_cnt: IntGauge::new(
                "kafka_broker_waitresp_cnt",
                "Number of requests in-flight",
            )?,
            waitresp_msg_cnt: IntGauge::new(
                "kafka_broker_waitresp_msg_cnt",
                "Number of messages in-flight",
            )?,
            tx: IntCounter::new("kafka_broker_tx_total", "Total broker transmissions")?,
            tx_bytes: IntCounter::new(
                "kafka_broker_tx_bytes_total",
                "Total broker bytes transmitted",
            )?,
            tx_errs: IntCounter::new(
                "kafka_broker_tx_errs_total",
                "Total broker transmission errors",
            )?,
            tx_retries: IntCounter::new(
                "kafka_broker_tx_retries_total",
                "Total broker transmission retries",
            )?,
            req_timeouts: IntCounter::new(
                "kafka_broker_req_timeouts_total",
                "Total broker request timeouts",
            )?,
            rx: IntCounter::new("kafka_broker_rx_total", "Total broker receptions")?,
            rx_bytes: IntCounter::new(
                "kafka_broker_rx_bytes_total",
                "Total broker bytes received",
            )?,
            rx_errs: IntCounter::new(
                "kafka_broker_rx_errs_total",
                "Total broker reception errors",
            )?,
            rx_corrid_errs: IntCounter::new(
                "kafka_broker_rx_corrid_errs_total",
                "Total broker correlation ID errors",
            )?,
            rx_partial: IntCounter::new(
                "kafka_broker_rx_partial_total",
                "Total broker partial message sets",
            )?,
            connects: IntCounter::new(
                "kafka_broker_connects_total",
                "Total broker connection attempts",
            )?,
            disconnects: IntCounter::new(
                "kafka_broker_disconnects_total",
                "Total broker disconnections",
            )?,
            int_latency: Histogram::with_opts(HistogramOpts::new(
                "kafka_broker_int_latency",
                "Internal broker latency",
            ))?,
            outbuf_latency: Histogram::with_opts(HistogramOpts::new(
                "kafka_broker_outbuf_latency",
                "Outbuf latency",
            ))?,
            rtt: Histogram::with_opts(HistogramOpts::new(
                "kafka_broker_rtt",
                "Broker round-trip time",
            ))?,
            throttle: Histogram::with_opts(HistogramOpts::new(
                "kafka_broker_throttle",
                "Broker throttle time",
            ))?,
        })
    }
}

impl TopicMetrics {
    fn new(labels: &[&str], descs: &mut Vec<Desc>) -> Self {
        Self {
            metadata_age: KafkaMetricsCollector::create_gauge_vec(
                "kafka_topic_metadata_age",
                "Age of topic metadata",
                labels,
                descs,
            ),
            batchsize: KafkaMetricsCollector::create_histogram_vec(
                "kafka_topic_batchsize",
                "Topic batch sizes",
                labels,
                descs,
            ),
            batchcnt: KafkaMetricsCollector::create_histogram_vec(
                "kafka_topic_batchcnt",
                "Topic batch counts",
                labels,
                descs,
            ),
        }
    }
}

impl PartitionMetrics {
    fn new(labels: &[&str], descs: &mut Vec<Desc>) -> Self {
        Self {
            msgq_cnt: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_msgq_cnt",
                "Messages in partition queue",
                labels,
                descs,
            ),
            msgq_bytes: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_msgq_bytes",
                "Bytes in partition queue",
                labels,
                descs,
            ),
            xmit_msgq_cnt: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_xmit_msgq_cnt",
                "Messages in partition transmit queue",
                labels,
                descs,
            ),
            xmit_msgq_bytes: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_xmit_msgq_bytes",
                "Bytes in partition transmit queue",
                labels,
                descs,
            ),
            fetchq_cnt: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_fetchq_cnt",
                "Messages in partition fetch queue",
                labels,
                descs,
            ),
            fetchq_size: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_fetchq_size",
                "Size of partition fetch queue",
                labels,
                descs,
            ),
            query_offset: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_query_offset",
                "Current partition query offset",
                labels,
                descs,
            ),
            next_offset: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_next_offset",
                "Next partition offset",
                labels,
                descs,
            ),
            app_offset: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_app_offset",
                "Application partition offset",
                labels,
                descs,
            ),
            stored_offset: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_stored_offset",
                "Stored partition offset",
                labels,
                descs,
            ),
            committed_offset: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_committed_offset",
                "Committed partition offset",
                labels,
                descs,
            ),
            eof_offset: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_eof_offset",
                "EOF partition offset",
                labels,
                descs,
            ),
            lo_offset: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_lo_offset",
                "Low watermark partition offset",
                labels,
                descs,
            ),
            hi_offset: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_hi_offset",
                "High watermark partition offset",
                labels,
                descs,
            ),
            consumer_lag: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_consumer_lag",
                "Consumer lag",
                labels,
                descs,
            ),
            consumer_lag_stored: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_consumer_lag_stored",
                "Stored consumer lag",
                labels,
                descs,
            ),
            txmsgs: KafkaMetricsCollector::create_counter_vec(
                "kafka_partition_txmsgs_total",
                "Total partition messages transmitted",
                labels,
                descs,
            ),
            txbytes: KafkaMetricsCollector::create_counter_vec(
                "kafka_partition_txbytes_total",
                "Total partition bytes transmitted",
                labels,
                descs,
            ),
            rxmsgs: KafkaMetricsCollector::create_counter_vec(
                "kafka_partition_rxmsgs_total",
                "Total partition messages received",
                labels,
                descs,
            ),
            rxbytes: KafkaMetricsCollector::create_counter_vec(
                "kafka_partition_rxbytes_total",
                "Total partition bytes received",
                labels,
                descs,
            ),
            msgs: KafkaMetricsCollector::create_counter_vec(
                "kafka_partition_msgs_total",
                "Total partition messages",
                labels,
                descs,
            ),
            rx_ver_drops: KafkaMetricsCollector::create_counter_vec(
                "kafka_partition_rx_ver_drops_total",
                "Total partition version drops",
                labels,
                descs,
            ),
            msgs_inflight: KafkaMetricsCollector::create_gauge_vec(
                "kafka_partition_msgs_inflight",
                "Messages in flight",
                labels,
                descs,
            ),
        }
    }

    fn collect_metrics(
        &self,
        topic_name: &str,
        partition_id: &i32,
        partition: &rdkafka::statistics::Partition,
    ) -> Vec<proto::MetricFamily> {
        let mut mfs = Vec::new();
        let labels = &[topic_name, &partition_id.to_string()];

        self.set_gauges(labels, partition);
        self.set_counters(labels, partition);

        self.collect_all_metrics(&mut mfs);

        mfs
    }

    fn set_gauges(&self, labels: &[&str], partition: &rdkafka::statistics::Partition) {
        self.msgq_cnt
            .with_label_values(labels)
            .set(partition.msgq_cnt);
        self.msgq_bytes
            .with_label_values(labels)
            .set(partition.msgq_bytes as i64);
        self.xmit_msgq_cnt
            .with_label_values(labels)
            .set(partition.xmit_msgq_cnt);
        self.xmit_msgq_bytes
            .with_label_values(labels)
            .set(partition.xmit_msgq_bytes as i64);
        self.fetchq_cnt
            .with_label_values(labels)
            .set(partition.fetchq_cnt);
        self.fetchq_size
            .with_label_values(labels)
            .set(partition.fetchq_size as i64);
        self.query_offset
            .with_label_values(labels)
            .set(partition.query_offset);
        self.next_offset
            .with_label_values(labels)
            .set(partition.next_offset);
        self.app_offset
            .with_label_values(labels)
            .set(partition.app_offset);
        self.stored_offset
            .with_label_values(labels)
            .set(partition.stored_offset);
        self.committed_offset
            .with_label_values(labels)
            .set(partition.committed_offset);
        self.eof_offset
            .with_label_values(labels)
            .set(partition.eof_offset);
        self.lo_offset
            .with_label_values(labels)
            .set(partition.lo_offset);
        self.hi_offset
            .with_label_values(labels)
            .set(partition.hi_offset);
        self.consumer_lag
            .with_label_values(labels)
            .set(partition.consumer_lag);
        self.consumer_lag_stored
            .with_label_values(labels)
            .set(partition.consumer_lag_stored);
        self.msgs_inflight
            .with_label_values(labels)
            .set(partition.msgs_inflight);
    }

    fn set_counters(&self, labels: &[&str], partition: &rdkafka::statistics::Partition) {
        self.txmsgs
            .with_label_values(labels)
            .inc_by(partition.txmsgs);
        self.txbytes
            .with_label_values(labels)
            .inc_by(partition.txbytes);
        self.rxmsgs
            .with_label_values(labels)
            .inc_by(partition.rxmsgs);
        self.rxbytes
            .with_label_values(labels)
            .inc_by(partition.rxbytes);
        self.msgs.with_label_values(labels).inc_by(partition.msgs);
        self.rx_ver_drops
            .with_label_values(labels)
            .inc_by(partition.rx_ver_drops);
    }

    fn collect_all_metrics(&self, mfs: &mut Vec<proto::MetricFamily>) {
        // Collect gauges
        mfs.extend(self.msgq_cnt.collect());
        mfs.extend(self.msgq_bytes.collect());
        mfs.extend(self.xmit_msgq_cnt.collect());
        mfs.extend(self.xmit_msgq_bytes.collect());
        mfs.extend(self.fetchq_cnt.collect());
        mfs.extend(self.fetchq_size.collect());
        mfs.extend(self.query_offset.collect());
        mfs.extend(self.next_offset.collect());
        mfs.extend(self.app_offset.collect());
        mfs.extend(self.stored_offset.collect());
        mfs.extend(self.committed_offset.collect());
        mfs.extend(self.eof_offset.collect());
        mfs.extend(self.lo_offset.collect());
        mfs.extend(self.hi_offset.collect());
        mfs.extend(self.consumer_lag.collect());
        mfs.extend(self.consumer_lag_stored.collect());
        mfs.extend(self.msgs_inflight.collect());

        // Collect counters
        mfs.extend(self.txmsgs.collect());
        mfs.extend(self.txbytes.collect());
        mfs.extend(self.rxmsgs.collect());
        mfs.extend(self.rxbytes.collect());
        mfs.extend(self.msgs.collect());
        mfs.extend(self.rx_ver_drops.collect());
    }
}

impl BrokerMetrics {
    fn collect_metrics(&self, broker: &rdkafka::statistics::Broker) -> Vec<proto::MetricFamily> {
        let mut mfs = Vec::new();

        self.state_cnt.set(match broker.state.as_str() {
            "UP" => 1,
            "DOWN" => 0,
            _ => -1,
        });

        self.set_gauges(broker);
        self.set_counters(broker);
        self.set_latency_metrics(broker);
        self.collect_all_metrics(&mut mfs);

        mfs
    }

    fn set_gauges(&self, broker: &rdkafka::statistics::Broker) {
        self.outbuf_cnt.set(broker.outbuf_cnt);
        self.outbuf_msg_cnt.set(broker.outbuf_msg_cnt);
        self.waitresp_cnt.set(broker.waitresp_cnt);
        self.waitresp_msg_cnt.set(broker.waitresp_msg_cnt);
    }

    fn set_counters(&self, broker: &rdkafka::statistics::Broker) {
        self.tx.inc_by(broker.tx);
        self.tx_bytes.inc_by(broker.txbytes);
        self.tx_errs.inc_by(broker.txerrs);
        self.tx_retries.inc_by(broker.txretries);
        self.req_timeouts.inc_by(broker.req_timeouts);
        self.rx.inc_by(broker.rx);
        self.rx_bytes.inc_by(broker.rxbytes);
        self.rx_errs.inc_by(broker.rxerrs);
        self.rx_corrid_errs.inc_by(broker.rxcorriderrs);
        self.rx_partial.inc_by(broker.rxpartial);

        if let Some(connects) = broker.connects {
            self.connects.inc_by(connects as u64);
        }
        if let Some(disconnects) = broker.disconnects {
            self.disconnects.inc_by(disconnects as u64);
        }
    }

    fn set_latency_metrics(&self, broker: &rdkafka::statistics::Broker) {
        if let Some(ref latency) = broker.int_latency {
            self.int_latency.observe(latency.avg as f64);
        }
        if let Some(ref latency) = broker.outbuf_latency {
            self.outbuf_latency.observe(latency.avg as f64);
        }
        if let Some(ref rtt) = broker.rtt {
            self.rtt.observe(rtt.avg as f64);
        }
        if let Some(ref throttle) = broker.throttle {
            self.throttle.observe(throttle.avg as f64);
        }
    }

    fn collect_all_metrics(&self, mfs: &mut Vec<proto::MetricFamily>) {
        mfs.extend(self.state_cnt.collect());
        mfs.extend(self.outbuf_cnt.collect());
        mfs.extend(self.outbuf_msg_cnt.collect());
        mfs.extend(self.waitresp_cnt.collect());
        mfs.extend(self.waitresp_msg_cnt.collect());
        mfs.extend(self.tx.collect());
        mfs.extend(self.tx_bytes.collect());
        mfs.extend(self.tx_errs.collect());
        mfs.extend(self.tx_retries.collect());
        mfs.extend(self.req_timeouts.collect());
        mfs.extend(self.rx.collect());
        mfs.extend(self.rx_bytes.collect());
        mfs.extend(self.rx_errs.collect());
        mfs.extend(self.rx_corrid_errs.collect());
        mfs.extend(self.rx_partial.collect());
        mfs.extend(self.connects.collect());
        mfs.extend(self.disconnects.collect());
        mfs.extend(self.int_latency.collect());
        mfs.extend(self.outbuf_latency.collect());
        mfs.extend(self.rtt.collect());
        mfs.extend(self.throttle.collect());
    }
}

impl TopicMetrics {
    fn collect_metrics(
        &self,
        topic_name: &str,
        topic: &rdkafka::statistics::Topic,
    ) -> Vec<proto::MetricFamily> {
        let mut mfs = Vec::new();
        let labels = &[topic_name];

        self.metadata_age
            .with_label_values(labels)
            .set(topic.metadata_age);
        self.batchsize
            .with_label_values(labels)
            .observe(topic.batchsize.avg as f64);
        self.batchcnt
            .with_label_values(labels)
            .observe(topic.batchcnt.avg as f64);

        mfs.extend(self.metadata_age.collect());
        mfs.extend(self.batchsize.collect());
        mfs.extend(self.batchcnt.collect());

        mfs
    }
}

impl ConsumerGroupMetrics {
    fn collect_metrics(
        &self,
        cgrp: &rdkafka::statistics::ConsumerGroup,
    ) -> Vec<proto::MetricFamily> {
        let mut mfs = Vec::new();

        self.rebalance_cnt.inc_by(cgrp.rebalance_cnt as u64);
        self.rebalance_age.set(cgrp.rebalance_age);
        self.assignment_size.set(cgrp.assignment_size as i64);

        mfs.extend(self.rebalance_cnt.collect());
        mfs.extend(self.rebalance_age.collect());
        mfs.extend(self.assignment_size.collect());

        mfs
    }
}

impl EosMetrics {
    fn collect_metrics(
        &self,
        eos: &rdkafka::statistics::ExactlyOnceSemantics,
    ) -> Vec<proto::MetricFamily> {
        let mut mfs = Vec::new();

        self.epoch_cnt.inc_by(eos.epoch_cnt as u64);
        self.producer_id.set(eos.producer_id);
        self.producer_epoch.set(eos.producer_epoch);

        mfs.extend(self.epoch_cnt.collect());
        mfs.extend(self.producer_id.collect());
        mfs.extend(self.producer_epoch.collect());

        mfs
    }
}

impl CoreMetrics {
    fn collect_metrics(&self, stats: &Statistics) -> Vec<proto::MetricFamily> {
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

        mfs
    }
}

impl ConsumerGroupMetrics {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            rebalance_cnt: IntCounter::new("kafka_cgrp_rebalance_total", "Total rebalances")?,
            rebalance_age: IntGauge::new("kafka_cgrp_rebalance_age", "Rebalance age")?,
            assignment_size: IntGauge::new("kafka_cgrp_assignment_size", "Assignment size")?,
        })
    }
}

impl EosMetrics {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            epoch_cnt: IntCounter::new("kafka_eos_epoch_total", "Total number of epochs")?,
            producer_id: IntGauge::new("kafka_eos_producer_id", "Producer ID")?,
            producer_epoch: IntGauge::new("kafka_eos_producer_epoch", "Producer epoch")?,
        })
    }
}

impl KafkaMetricsCollector {
    pub fn new(stats: Arc<RwLock<Statistics>>) -> anyhow::Result<KafkaMetricsCollector> {
        let mut descs = Vec::new();
        let topic_labels = &["topic"];
        let partition_labels = &["topic", "partition"];

        let core_metrics = CoreMetrics::new()?;
        let broker_metrics = BrokerMetrics::new()?;
        let topic_metrics = TopicMetrics::new(topic_labels, &mut descs);
        let partition_metrics = PartitionMetrics::new(partition_labels, &mut descs);
        let consumer_metrics = ConsumerGroupMetrics::new()?;
        let eos_metrics = EosMetrics::new()?;

        Ok(KafkaMetricsCollector {
            stats,
            descs,
            core_metrics,
            broker_metrics,
            topic_metrics,
            partition_metrics,
            consumer_metrics,
            eos_metrics,
        })
    }

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

        let mut mfs = Vec::new();

        // Collect core metrics
        mfs.extend(self.core_metrics.collect_metrics(&stats));

        // Collect broker metrics
        for (_broker_id, broker) in stats.brokers.iter() {
            mfs.extend(self.broker_metrics.collect_metrics(broker));
        }

        // Collect topic and partition metrics
        for (topic_name, topic) in stats.topics.iter() {
            mfs.extend(self.topic_metrics.collect_metrics(topic_name, topic));

            // Collect partition metrics for each topic
            for (partition_id, partition) in topic.partitions.iter() {
                mfs.extend(self.partition_metrics.collect_metrics(
                    topic_name,
                    partition_id,
                    partition,
                ));
            }
        }

        // Collect consumer group metrics
        if let Some(ref cgrp) = stats.cgrp {
            mfs.extend(self.consumer_metrics.collect_metrics(cgrp));
        }

        // Collect EOS metrics
        if let Some(ref eos) = stats.eos {
            mfs.extend(self.eos_metrics.collect_metrics(eos));
        }

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
