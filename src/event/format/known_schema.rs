use std::collections::HashMap;

use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Map, Value};
use tracing::{error, warn};

const FORMATS_JSON: &str = include_str!("../../../resources/formats.json");

// Schema definition with pattern matching
pub static KNOWN_SCHEMA_LIST: Lazy<EventProcessor> = Lazy::new(|| {
    let mut processor = EventProcessor {
        schema_definitions: HashMap::new(),
    };

    // Register known schemas
    processor.register_schema();

    processor
});

#[derive(Debug)]
pub struct SchemaDefinition {
    pattern: Option<Regex>,
    field_mappings: Vec<String>, // Maps field names to regex capture groups
}

impl SchemaDefinition {
    pub fn extract(&self, event: &str) -> Option<Map<String, Value>> {
        if let Some(pattern) = &self.pattern {
            if let Some(captures) = pattern.captures(event) {
                let mut extracted_fields = Map::new();

                // With named capture groups, you can iterate over the field names
                for field_name in self.field_mappings.iter() {
                    if let Some(value) = captures.name(field_name) {
                        extracted_fields.insert(
                            field_name.to_owned(),
                            Value::String(value.as_str().to_string()),
                        );
                    }
                }

                return Some(extracted_fields);
            }
        }

        None
    }
}

#[derive(Debug, Deserialize)]
struct Format {
    name: String,
    regex: Vec<Pattern>,
}
#[derive(Debug, Deserialize)]
struct Pattern {
    pattern: Option<String>,
    fields: Vec<String>,
}

#[derive(Debug)]
pub struct EventProcessor {
    pub schema_definitions: HashMap<String, SchemaDefinition>,
}

impl EventProcessor {
    fn register_schema(&mut self) {
        let json_data: serde_json::Value = serde_json::from_str(FORMATS_JSON).unwrap();
        let formats: Vec<Format> =
            serde_json::from_value(json_data).expect("Failed to parse formats.json");

        for format in formats {
            for pattern in &format.regex {
                if let Some(pattern_str) = &pattern.pattern {
                    // Compile the regex pattern
                    match Regex::new(pattern_str) {
                        Ok(exp) => {
                            let field_mappings = pattern
                                .fields
                                .iter()
                                .map(|field| field.to_string())
                                .collect();

                            self.schema_definitions.insert(
                                format.name.clone(),
                                SchemaDefinition {
                                    pattern: Some(exp),
                                    field_mappings,
                                },
                            );
                        }
                        Err(e) => {
                            error!("Error compiling regex pattern: {e}; Pattern: {pattern_str}");
                        }
                    }
                } else {
                    let field_mappings = pattern
                        .fields
                        .iter()
                        .map(|field| field.to_string())
                        .collect();

                    self.schema_definitions.insert(
                        format.name.clone(),
                        SchemaDefinition {
                            pattern: None,
                            field_mappings,
                        },
                    );
                }
            }
        }
    }
}
pub fn extract_from_inline_log(
    mut json: Value,
    log_source: &str,
    extract_log: Option<&str>,
) -> Value {
    let Some(schema) = KNOWN_SCHEMA_LIST.schema_definitions.get(log_source) else {
        warn!("Unknown log format: {log_source}");
        return json;
    };

    match &mut json {
        Value::Array(list) => {
            for event in list {
                let Value::Object(event) = event else {
                    continue;
                };
                per_event_extraction(event, schema, extract_log)
            }
        }
        Value::Object(event) => per_event_extraction(event, schema, extract_log),
        _ => unreachable!("We don't accept events of the form: {json}"),
    }

    json
}

pub fn per_event_extraction(
    obj: &mut Map<String, Value>,
    schema: &SchemaDefinition,
    extract_log: Option<&str>,
) {
    if let Some(event) = extract_log
        .and_then(|field| obj.get(field))
        .and_then(|s| s.as_str())
    {
        if let Some(additional) = schema.extract(event) {
            obj.extend(additional);
        }
    }
}
