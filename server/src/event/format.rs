/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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
 *
 */

use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Error as AnyError};
use arrow_array::{RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::Utc;

use crate::utils::{self, arrow::get_field};

use super::{DEFAULT_METADATA_KEY, DEFAULT_TAGS_KEY, DEFAULT_TIMESTAMP_KEY};

pub mod json;

type Tags = String;
type Metadata = String;

// Global Trait for event format
// This trait is implemented by all the event formats
pub trait EventFormat: Sized {
    type Data;
    fn to_data(
        self,
        schema: &HashMap<String, Field>,
    ) -> Result<(Self::Data, Schema, bool, Tags, Metadata), AnyError>;
    fn decode(data: Self::Data, schema: Arc<Schema>) -> Result<RecordBatch, AnyError>;
    fn into_recordbatch(
        self,
        schema: &HashMap<String, Field>,
    ) -> Result<(RecordBatch, bool), AnyError> {
        let (data, mut schema, is_first, tags, metadata) = self.to_data(schema)?;

        if get_field(&schema, DEFAULT_TAGS_KEY).is_some() {
            return Err(anyhow!("field {} is a reserved field", DEFAULT_TAGS_KEY));
        };

        if get_field(&schema, DEFAULT_TAGS_KEY).is_some() {
            return Err(anyhow!(
                "field {} is a reserved field",
                DEFAULT_METADATA_KEY
            ));
        };

        if get_field(&schema, DEFAULT_TAGS_KEY).is_some() {
            return Err(anyhow!(
                "field {} is a reserved field",
                DEFAULT_TIMESTAMP_KEY
            ));
        };

        // add the p_timestamp field to the event schema to the 0th index
        schema.fields.insert(
            0,
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        );

        // p_tags and p_metadata are added to the end of the schema
        let tags_index = schema.fields.len();
        let metadata_index = tags_index + 1;
        schema
            .fields
            .push(Field::new(DEFAULT_TAGS_KEY, DataType::Utf8, true));
        schema
            .fields
            .push(Field::new(DEFAULT_METADATA_KEY, DataType::Utf8, true));

        // prepare the record batch and new fields to be added
        let schema_ref = Arc::new(schema);
        let rb = Self::decode(data, Arc::clone(&schema_ref))?;
        let tags_arr = StringArray::from_iter_values(std::iter::repeat(&tags).take(rb.num_rows()));
        let metadata_arr =
            StringArray::from_iter_values(std::iter::repeat(&metadata).take(rb.num_rows()));
        let timestamp_array = get_timestamp_array(rb.num_rows());

        // modify the record batch to add fields to respective indexes
        let rb = utils::arrow::replace_columns(
            Arc::clone(&schema_ref),
            rb,
            &[0, tags_index, metadata_index],
            &[
                Arc::new(timestamp_array),
                Arc::new(tags_arr),
                Arc::new(metadata_arr),
            ],
        );

        Ok((rb, is_first))
    }
}

fn get_timestamp_array(size: usize) -> TimestampMillisecondArray {
    let time = Utc::now();
    TimestampMillisecondArray::from_value(time.timestamp_millis(), size)
}
