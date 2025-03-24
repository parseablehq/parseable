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

use datafusion::arrow::record_batch::RecordBatch;
use serde_json::{json, Value};

use crate::{handlers::http::query::QueryError, utils::arrow::record_batches_to_json};

pub struct QueryResponse {
    pub records: Vec<RecordBatch>,
    pub fields: Vec<String>,
    pub fill_null: bool,
    pub with_fields: bool,
}

impl QueryResponse {
    /// Convert into a JSON response
    pub fn to_json(&self) -> Result<Value, QueryError> {
        let mut json = record_batches_to_json(&self.records)?;

        if self.fill_null {
            for object in json.iter_mut() {
                for field in self.fields.iter() {
                    object.entry(field).or_insert(Value::Null);
                }
            }
        }

        let json = if self.with_fields {
            json!({
                "fields": self.fields,
                "records": json
            })
        } else {
            json!(json)
        };

        Ok(json)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::Schema;
    use serde_json::{json, Value};

    use crate::response::QueryResponse;

    #[test]
    fn check_empty_record_batches_to_json() {
        let response = QueryResponse {
            records: vec![RecordBatch::new_empty(Arc::new(Schema::empty()))],
            fields: vec![],
            fill_null: false,
            with_fields: false,
        };

        assert_eq!(response.to_json().unwrap(), Value::Array(vec![]));
    }

    #[test]
    fn check_record_batches_to_json() {
        let array1: Arc<dyn Array> = Arc::new(Int64Array::from_iter(0..3));
        let array2: Arc<dyn Array> = Arc::new(Float64Array::from_iter((0..3).map(|x| x as f64)));
        let array3: Arc<dyn Array> = Arc::new(StringArray::from_iter(
            (0..3).map(|x| Some(format!("str {x}"))),
        ));

        let record = RecordBatch::try_from_iter_with_nullable([
            ("a", array1, true),
            ("b", array2, true),
            ("c", array3, true),
        ])
        .unwrap();
        let response = QueryResponse {
            records: vec![record],
            fields: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            fill_null: false,
            with_fields: false,
        };

        assert_eq!(
            response.to_json().unwrap(),
            json!([
                {"a": 0, "b": 0.0, "c": "str 0"},
                {"a": 1, "b": 1.0, "c": "str 1"},
                {"a": 2, "b": 2.0, "c": "str 2"}
            ])
        );
    }

    #[test]
    fn check_record_batches_to_json_with_fields() {
        let array1: Arc<dyn Array> = Arc::new(Int64Array::from_iter(0..3));
        let array2: Arc<dyn Array> = Arc::new(Float64Array::from_iter((0..3).map(|x| x as f64)));
        let array3: Arc<dyn Array> = Arc::new(StringArray::from_iter(
            (0..3).map(|x| Some(format!("str {x}"))),
        ));

        let record = RecordBatch::try_from_iter_with_nullable([
            ("a", array1, true),
            ("b", array2, true),
            ("c", array3, true),
        ])
        .unwrap();
        let response = QueryResponse {
            records: vec![record],
            fields: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            fill_null: false,
            with_fields: true,
        };

        assert_eq!(
            response.to_json().unwrap(),
            json!({
                "fields": ["a", "b", "c"],
                "records": [
                    {"a": 0, "b": 0.0, "c": "str 0"},
                    {"a": 1, "b": 1.0, "c": "str 1"},
                    {"a": 2, "b": 2.0, "c": "str 2"}
                ]
            })
        );
    }

    #[test]
    fn check_record_batches_to_json_without_nulls() {
        let array1: Arc<dyn Array> = Arc::new(Int64Array::from_iter(0..3));
        let array2: Arc<dyn Array> = Arc::new(Float64Array::from_iter((0..3).map(|x| x as f64)));
        let array3: Arc<dyn Array> = Arc::new(StringArray::from_iter((0..3).map(|x| {
            if x == 1 {
                Some(format!("str {x}"))
            } else {
                None
            }
        })));

        let record = RecordBatch::try_from_iter_with_nullable([
            ("a", array1, true),
            ("b", array2, true),
            ("c", array3, true),
        ])
        .unwrap();
        let response = QueryResponse {
            records: vec![record],
            fields: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            fill_null: false,
            with_fields: false,
        };

        assert_eq!(
            response.to_json().unwrap(),
            json!([
                {"a": 0, "b": 0.0},
                {"a": 1, "b": 1.0, "c": "str 1"},
                {"a": 2, "b": 2.0}
            ])
        );
    }

    #[test]
    fn check_record_batches_to_json_with_nulls() {
        let array1: Arc<dyn Array> = Arc::new(Int64Array::from_iter(0..3));
        let array2: Arc<dyn Array> = Arc::new(Float64Array::from_iter((0..3).map(|x| x as f64)));
        let array3: Arc<dyn Array> = Arc::new(StringArray::from_iter((0..3).map(|x| {
            if x == 1 {
                Some(format!("str {x}"))
            } else {
                None
            }
        })));

        let record = RecordBatch::try_from_iter_with_nullable([
            ("a", array1, true),
            ("b", array2, true),
            ("c", array3, true),
        ])
        .unwrap();
        let response = QueryResponse {
            records: vec![record],
            fields: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            fill_null: true,
            with_fields: false,
        };

        assert_eq!(
            response.to_json().unwrap(),
            json!([
                {"a": 0, "b": 0.0, "c": null},
                {"a": 1, "b": 1.0, "c": "str 1"},
                {"a": 2, "b": 2.0, "c": null}
            ])
        );
    }
}
