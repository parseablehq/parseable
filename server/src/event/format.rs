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
 *
 */

use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Error as AnyError};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::utils::{self, arrow::get_field};

use super::{DEFAULT_METADATA_KEY, DEFAULT_TAGS_KEY, DEFAULT_TIMESTAMP_KEY};

pub mod json;

type Tags = String;
type Metadata = String;
type EventSchema = Vec<Arc<Field>>;

// Global Trait for event format
// This trait is implemented by all the event formats
pub trait EventFormat: Sized {
    type Data;

    fn to_data(
        self,
        schema: HashMap<String, Arc<Field>>,
        time_partition: Option<String>,
    ) -> Result<(Self::Data, EventSchema, bool, Tags, Metadata), AnyError>;
    fn decode(data: Self::Data, schema: Arc<Schema>) -> Result<RecordBatch, AnyError>;
    fn into_recordbatch(
        self,
        schema: HashMap<String, Arc<Field>>,
        time_partition: Option<String>,
    ) -> Result<(RecordBatch, bool), AnyError> {
        let (data, mut schema, is_first, tags, metadata) = self.to_data(schema, time_partition)?;

        if get_field(&schema, DEFAULT_TAGS_KEY).is_some() {
            return Err(anyhow!("field {} is a reserved field", DEFAULT_TAGS_KEY));
        };

        if get_field(&schema, DEFAULT_METADATA_KEY).is_some() {
            return Err(anyhow!(
                "field {} is a reserved field",
                DEFAULT_METADATA_KEY
            ));
        };

        if get_field(&schema, DEFAULT_TIMESTAMP_KEY).is_some() {
            return Err(anyhow!(
                "field {} is a reserved field",
                DEFAULT_TIMESTAMP_KEY
            ));
        };

        // add the p_timestamp field to the event schema to the 0th index
        schema.insert(
            0,
            Arc::new(Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
        );

        // p_tags and p_metadata are added to the end of the schema
        let tags_index = schema.len();
        let metadata_index = tags_index + 1;
        schema.push(Arc::new(Field::new(DEFAULT_TAGS_KEY, DataType::Utf8, true)));
        schema.push(Arc::new(Field::new(
            DEFAULT_METADATA_KEY,
            DataType::Utf8,
            true,
        )));

        // prepare the record batch and new fields to be added
        let schema = Arc::new(Schema::new(schema));
        let rb = Self::decode(data, schema.clone())?;
        let tags_arr = StringArray::from_iter_values(std::iter::repeat(&tags).take(rb.num_rows()));
        let metadata_arr =
            StringArray::from_iter_values(std::iter::repeat(&metadata).take(rb.num_rows()));
        // modify the record batch to add fields to respective indexes
        let rb = utils::arrow::replace_columns(
            Arc::clone(&schema),
            &rb,
            &[tags_index, metadata_index],
            &[Arc::new(tags_arr), Arc::new(metadata_arr)],
        );

        Ok((rb, is_first))
    }
}
