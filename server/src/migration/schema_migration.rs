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

use std::collections::HashMap;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use itertools::Itertools;
use md5::Digest;

pub(super) fn v1_v2(schema: Option<Schema>) -> anyhow::Result<HashMap<String, Schema>> {
    let Some(schema) = schema else { return Ok(HashMap::new()) };
    let schema = Schema::try_merge(vec![
        Schema::new(vec![Field::new(
            "p_timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]),
        schema,
    ])?;

    let list_of_fields = schema
        .fields()
        .iter()
        // skip p_timestamp
        .skip(1)
        .map(|f| f.name())
        .sorted();
    let mut hasher = md5::Md5::new();
    list_of_fields.for_each(|field| hasher.update(field.as_bytes()));
    let key = hex::encode(hasher.finalize());
    let mut map = HashMap::new();
    map.insert(key, schema);

    Ok(map)
}
