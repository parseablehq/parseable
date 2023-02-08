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
