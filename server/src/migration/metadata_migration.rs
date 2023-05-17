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
 */

use std::vec;

use serde_json::Value;

pub fn v1_v2(mut storage_metadata: serde_json::Value) -> Value {
    let metadata = storage_metadata.as_object_mut().unwrap();
    *metadata.get_mut("version").unwrap() = Value::String("v2".to_string());
    metadata.remove("user");
    metadata.remove("stream");
    metadata.insert("users".to_string(), Value::Array(vec![]));
    metadata.insert("streams".to_string(), Value::Array(vec![]));
    storage_metadata
}
