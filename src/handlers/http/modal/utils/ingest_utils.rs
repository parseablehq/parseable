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
use serde_json::Value;

use crate::{
    event::format::{json, EventFormat, LogSource},
    handlers::http::ingest::PostError,
    parseable::PARSEABLE,
    storage::StreamType,
};

pub async fn push_logs(
    stream_name: &str,
    json: Value,
    log_source: &LogSource,
) -> Result<(), PostError> {
    let stream = PARSEABLE.get_stream(stream_name)?;
    let time_partition = stream.get_time_partition();
    let time_partition_limit = PARSEABLE
        .get_stream(stream_name)?
        .get_time_partition_limit();
    let static_schema_flag = stream.get_static_schema_flag();
    let custom_partition = stream.get_custom_partition();
    let schema_version = stream.get_schema_version();
    let p_timestamp = Utc::now();

        let origin_size = serde_json::to_vec(&json).unwrap().len() as u64; // string length need not be the same as byte length
        let schema = PARSEABLE.get_stream(stream_name)?.get_schema_raw();
        json::Event { json, p_timestamp }
            .into_event(
                stream_name.to_owned(),
                origin_size,
                &schema,
                static_schema_flag,
                custom_partition.as_ref(),
                time_partition.as_ref(),
                time_partition_limit,
                schema_version,
                log_source,
                StreamType::UserDefined,
            )?
            .process()?;
    

    Ok(())
}
