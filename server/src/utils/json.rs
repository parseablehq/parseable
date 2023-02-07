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

use serde_json;
use serde_json::json;
use serde_json::Value;

pub fn flatten_json_body(body: &serde_json::Value) -> Result<Value, serde_json::Error> {
    let mut flat_value: Value = json!({});
    flatten_json::flatten(body, &mut flat_value, None, false, Some("_")).unwrap();
    Ok(flat_value)
}

pub fn merge(value: &mut Value, fields: impl Iterator<Item = (String, Value)>) {
    if let Value::Object(m) = value {
        for (k, v) in fields {
            match m.get_mut(&k) {
                Some(val) => {
                    *val = v;
                }
                None => {
                    m.insert(k, v);
                }
            }
        }
    }
}
