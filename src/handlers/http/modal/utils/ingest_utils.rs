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

use chrono::Utc;
use opentelemetry_proto::tonic::{
    logs::v1::LogsData, metrics::v1::MetricsData, trace::v1::TracesData,
};
use serde_json::Value;

use crate::{
    event::format::{json, EventFormat, LogSource},
    handlers::http::{
        ingest::PostError,
        kinesis::{flatten_kinesis_logs, Message},
    },
    otel::{logs::flatten_otel_logs, metrics::flatten_otel_metrics, traces::flatten_otel_traces},
    parseable::PARSEABLE,
    storage::StreamType,
    utils::json::{convert_array_to_object, flatten::convert_to_array},
};

pub async fn flatten_and_push_logs(
    json: Value,
    stream_name: &str,
    log_source: &LogSource,
) -> Result<(), PostError> {
    match log_source {
        LogSource::Kinesis => {
            //custom flattening required for Amazon Kinesis
            let message: Message = serde_json::from_value(json)?;
            for record in flatten_kinesis_logs(message) {
                push_logs(stream_name, record, &LogSource::default()).await?;
            }
        }
        LogSource::OtelLogs => {
            //custom flattening required for otel logs
            let logs: LogsData = serde_json::from_value(json)?;
            for record in flatten_otel_logs(&logs) {
                push_logs(stream_name, record, log_source).await?;
            }
        }
        LogSource::OtelTraces => {
            //custom flattening required for otel traces
            let traces: TracesData = serde_json::from_value(json)?;
            for record in flatten_otel_traces(&traces) {
                push_logs(stream_name, record, log_source).await?;
            }
        }
        LogSource::OtelMetrics => {
            //custom flattening required for otel metrics
            let metrics: MetricsData = serde_json::from_value(json)?;
            for record in flatten_otel_metrics(metrics) {
                push_logs(stream_name, record, log_source).await?;
            }
        }
        _ => {
            push_logs(stream_name, json, log_source).await?;
        }
    }
    Ok(())
}

async fn push_logs(
    stream_name: &str,
    json: Value,
    log_source: &LogSource,
) -> Result<(), PostError> {
    let stream = PARSEABLE.get_stream(stream_name)?;
    let time_partition = stream.get_time_partition();
    let time_partition_limit = stream.get_time_partition_limit();
    let static_schema_flag = stream.get_static_schema_flag();
    let custom_partitions = stream.get_custom_partitions();
    let schema_version = stream.get_schema_version();
    let p_timestamp = Utc::now();

    let data = if time_partition.is_some() || !custom_partitions.is_empty() {
        convert_array_to_object(
            json,
            time_partition.as_ref(),
            time_partition_limit,
            &custom_partitions,
            schema_version,
            log_source,
        )?
    } else {
        vec![convert_to_array(convert_array_to_object(
            json,
            None,
            None,
            &[],
            schema_version,
            log_source,
        )?)?]
    };

    for json in data {
        let origin_size = serde_json::to_vec(&json).unwrap().len() as u64; // string length need not be the same as byte length
        let schema = PARSEABLE.get_stream(stream_name)?.get_schema_raw();
        json::Event { json, p_timestamp }
            .into_event(
                stream_name.to_owned(),
                origin_size,
                &schema,
                static_schema_flag,
                &custom_partitions,
                time_partition.as_ref(),
                schema_version,
                StreamType::UserDefined,
            )?
            .process()?;
    }
    Ok(())
}
