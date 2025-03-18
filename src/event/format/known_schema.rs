/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::collections::HashMap;

use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Map, Value};
use tracing::{error, warn};

/// Predefined JSON with known textual logging formats
const FORMATS_JSON: &str = include_str!("../../../resources/formats.json");

/// Global instance of EventProcessor containing predefined schema definitions
pub static KNOWN_SCHEMA_LIST: Lazy<EventProcessor> =
    Lazy::new(|| EventProcessor::new(FORMATS_JSON));

/// Defines a schema for extracting structured data from logs using regular expressions
#[derive(Debug)]
pub struct SchemaDefinition {
    /// Regular expression pattern used to match and capture fields from log strings
    pattern: Option<Regex>,
    // Maps field names to regex capture groups
    field_mappings: Vec<String>,
}

impl SchemaDefinition {
    /// Extracts structured data from a log event string using a defined regex pattern
    ///
    /// # Arguments
    /// * `event` - The log event string to extract data from
    ///
    /// # Returns
    /// * `Some(Map<String, Value>)` - A map of field names to extracted values if extraction succeeds
    /// * `None` - If the pattern is missing or no matches were found
    pub fn extract(&self, event: &str) -> Option<Map<String, Value>> {
        let pattern = self.pattern.as_ref()?;
        let captures = pattern.captures(event)?;
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

        Some(extracted_fields)
    }

    /// Extracts JSON event from raw text in received message
    ///
    /// # Arguments
    /// * `obj` - The root level event object to extract into
    /// * `schema` - Schema definition to use for extraction
    /// * `extract_log` - Optional field name containing the raw log text
    pub fn per_event_extraction(&self, obj: &mut Map<String, Value>, extract_log: Option<&str>) {
        if let Some(additional) = extract_log
            .and_then(|field| obj.get(field))
            .and_then(|s| s.as_str())
            .and_then(|event| self.extract(event))
        {
            obj.extend(additional);
        }
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
    /// Parses given formats from JSON text and stores them in-memory
    fn new(json_text: &str) -> Self {
        let mut processor = EventProcessor {
            schema_definitions: HashMap::new(),
        };

        let formats: Vec<Format> =
            serde_json::from_str(json_text).expect("Known formats are stored as JSON text");

        for format in formats {
            for regex in &format.regex {
                // Compile the regex pattern if present
                // NOTE: we only warn if the pattern doesn't compile
                let pattern = regex.pattern.as_ref().and_then(|pattern| {
                    Regex::new(pattern)
                        .inspect_err(|err| {
                            error!("Error compiling regex pattern: {err}; Pattern: {pattern}")
                        })
                        .ok()
                });

                let field_mappings = regex.fields.clone();

                processor.schema_definitions.insert(
                    format.name.clone(),
                    SchemaDefinition {
                        pattern,
                        field_mappings,
                    },
                );
            }
        }

        processor
    }

    /// Extracts fields from logs embedded within a JSON string
    ///
    /// # Arguments
    /// * `json` - JSON value containing log entries
    /// * `log_source` - Name of the log format to use for extraction
    /// * `extract_log` - Optional field name containing the raw log text
    ///
    /// # Returns
    /// * `Value` - The original JSON with extracted fields added, if any
    pub fn extract_from_inline_log(
        &self,
        mut json: Value,
        log_source: &str,
        extract_log: Option<&str>,
    ) -> Value {
        let Some(schema) = self.schema_definitions.get(log_source) else {
            warn!("Unknown log format: {log_source}");
            return json;
        };

        match &mut json {
            Value::Array(list) => {
                for event in list {
                    let Value::Object(event) = event else {
                        continue;
                    };
                    schema.per_event_extraction(event, extract_log)
                }
            }
            Value::Object(event) => schema.per_event_extraction(event, extract_log),
            _ => unreachable!("We don't accept events of the form: {json}"),
        }

        json
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const TEST_CONFIG: &str = r#"
    [
        {
            "name": "apache_access",
            "regex": [
                {
                    "pattern": "^(?P<ip>[\\d.]+) - - \\[(?P<timestamp>[^\\]]+)\\] \"(?P<method>\\w+) (?P<path>[^\\s]+) HTTP/[\\d.]+\" (?P<status>\\d+) (?P<bytes>\\d+)",
                    "fields": ["ip", "timestamp", "method", "path", "status", "bytes"]
                }
            ]
        },
        {
            "name": "custom_app_log",
            "regex": [
                {
                    "pattern": "\\[(?P<level>\\w+)\\] \\[(?P<timestamp>[^\\]]+)\\] (?P<message>.*)",
                    "fields": ["level", "timestamp", "message"]
                }
            ]
        }
    ]
    "#;

    #[test]
    fn test_schema_load() {
        let processor = EventProcessor::new(TEST_CONFIG);
        assert_eq!(
            processor.schema_definitions.len(),
            2,
            "Expected 2 schema definitions"
        );

        assert!(processor.schema_definitions.contains_key("apache_access"));
        assert!(processor.schema_definitions.contains_key("custom_app_log"));
    }

    #[test]
    fn test_apache_log_extraction() {
        let processor = EventProcessor::new(TEST_CONFIG);

        let schema = processor.schema_definitions.get("apache_access").unwrap();
        let log =
            "192.168.1.1 - - [10/Oct/2023:13:55:36 +0000] \"GET /index.html HTTP/1.1\" 200 2326";

        let result = schema.extract(log);
        assert!(result.is_some(), "Failed to extract fields from valid log");

        let fields = result.unwrap();
        assert_eq!(fields.get("ip").unwrap().as_str().unwrap(), "192.168.1.1");
        assert_eq!(
            fields.get("timestamp").unwrap().as_str().unwrap(),
            "10/Oct/2023:13:55:36 +0000"
        );
        assert_eq!(fields.get("method").unwrap().as_str().unwrap(), "GET");
        assert_eq!(fields.get("path").unwrap().as_str().unwrap(), "/index.html");
        assert_eq!(fields.get("status").unwrap().as_str().unwrap(), "200");
        assert_eq!(fields.get("bytes").unwrap().as_str().unwrap(), "2326");
    }

    #[test]
    fn test_custom_log_extraction() {
        let processor = EventProcessor::new(TEST_CONFIG);

        let schema = processor.schema_definitions.get("custom_app_log").unwrap();
        let log = "[ERROR] [2023-10-10T13:55:36Z] Failed to connect to database";

        let result = schema.extract(log);
        assert!(result.is_some(), "Failed to extract fields from valid log");

        let fields = result.unwrap();
        assert_eq!(fields.get("level").unwrap().as_str().unwrap(), "ERROR");
        assert_eq!(
            fields.get("timestamp").unwrap().as_str().unwrap(),
            "2023-10-10T13:55:36Z"
        );
        assert_eq!(
            fields.get("message").unwrap().as_str().unwrap(),
            "Failed to connect to database"
        );
    }

    #[test]
    fn test_no_match() {
        let processor = EventProcessor::new(TEST_CONFIG);

        let schema = processor.schema_definitions.get("apache_access").unwrap();
        let log = "This is not an Apache log line";

        let result = schema.extract(log);
        assert!(
            result.is_none(),
            "Should not extract fields from invalid log format"
        );
    }

    #[test]
    fn test_extract_from_inline_log_object() {
        let processor = EventProcessor::new(TEST_CONFIG);

        let json_value = json!({
            "id": "12345",
            "raw_log": "[ERROR] [2023-10-10T13:55:36Z] Failed to connect to database"
        });

        let result =
            processor.extract_from_inline_log(json_value, "custom_app_log", Some("raw_log"));

        let obj = result.as_object().unwrap();
        assert!(obj.contains_key("level"));
        assert!(obj.contains_key("timestamp"));
        assert!(obj.contains_key("message"));
        assert_eq!(obj.get("level").unwrap().as_str().unwrap(), "ERROR");
    }

    #[test]
    fn test_extract_from_inline_log_array() {
        let processor = EventProcessor::new(TEST_CONFIG);

        let json_value = json!([
            {
                "id": "12345",
                "raw_log": "[ERROR] [2023-10-10T13:55:36Z] Failed to connect to database"
            },
            {
                "id": "12346",
                "raw_log": "[INFO] [2023-10-10T13:55:40Z] Application started"
            }
        ]);

        let result =
            processor.extract_from_inline_log(json_value, "custom_app_log", Some("raw_log"));

        let array = result.as_array().unwrap();
        assert_eq!(array.len(), 2);

        let first = array[0].as_object().unwrap();
        assert_eq!(first.get("level").unwrap().as_str().unwrap(), "ERROR");
        assert_eq!(
            first.get("message").unwrap().as_str().unwrap(),
            "Failed to connect to database"
        );

        let second = array[1].as_object().unwrap();
        assert_eq!(second.get("level").unwrap().as_str().unwrap(), "INFO");
        assert_eq!(
            second.get("message").unwrap().as_str().unwrap(),
            "Application started"
        );
    }

    #[test]
    fn test_unknown_log_format() {
        let processor = EventProcessor::new(TEST_CONFIG);
        let json_value = json!({
            "id": "12345",
            "raw_log": "Some log message"
        });

        let result =
            processor.extract_from_inline_log(json_value, "nonexistent_format", Some("raw_log"));

        // Should return original JSON without modification
        let obj = result.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert!(obj.contains_key("id"));
        assert!(obj.contains_key("raw_log"));
        assert!(!obj.contains_key("level"));
    }
}
