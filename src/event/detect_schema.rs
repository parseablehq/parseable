use std::collections::HashMap;

use once_cell::sync::OnceCell;
use regex::Regex;
use serde::Deserialize;

const FORMATS_JSON: &str = include_str!("format/formats.json");
// Schema definition with pattern matching
pub static KNOWN_SCHEMA_LIST: OnceCell<EventProcessor> = OnceCell::new();

#[derive(Debug)]
struct SchemaDefinition {
    name: String,
    pattern: Option<Regex>,
    field_mappings: Vec<String>, // Maps field names to regex capture groups
}
#[derive(Debug, Deserialize)]
struct Format {
    name: String,
    regex: Vec<Pattern>
}
#[derive(Debug, Deserialize)]
struct Pattern {
    pattern: Option<String>,
    fields: Vec<String>
}

#[derive(Debug)]
pub struct EventProcessor {
    schema_definitions: Vec<SchemaDefinition>,
}

impl EventProcessor {
    pub fn new() {
        let mut processor = EventProcessor {
            schema_definitions: Vec::new(),
        };
        
        // Register known schemas
        processor.register_schema();
        KNOWN_SCHEMA_LIST.set(processor).expect("only set once");
    }
    
    fn register_schema(&mut self) {
        let json_data: serde_json::Value = serde_json::from_str(FORMATS_JSON).unwrap();
        let formats: Vec<Format> =
            serde_json::from_value(json_data).expect("Failed to parse formats.json");
        
        for format in formats {
            let name = format.name;
            for pattern in format.regex {
                if let Some(pattern_str) = &pattern.pattern {
                    // Compile the regex pattern
                    match Regex::new(pattern_str) {
                        Ok(reg) => {
                            let field_mappings = pattern.fields.iter()
                                .map(|field| field.to_string())
                                .collect();
                            
                            self.schema_definitions.push(SchemaDefinition {
                                name: name.clone(),
                                pattern: Some(reg),
                                field_mappings,
                            });
                        },
                        Err(e) => {
                            eprintln!("Error compiling regex pattern: {}", e);
                            eprintln!("Pattern: {}", pattern_str);
                        }
                    }
                } else {
                    let field_mappings = pattern.fields.iter()
                        .map(|field| field.to_string())
                        .collect();
                    
                    self.schema_definitions.push(SchemaDefinition {
                        name: name.clone(),
                        pattern: None,
                        field_mappings,
                    });
                }
            }
        }
    }
    
    
}

pub fn detect_schema(event: &str, log_source: &str) -> Option<(String, Vec<String>)> {
    let processor = KNOWN_SCHEMA_LIST.get().expect("Schema processor not initialized");
    for schema in processor.schema_definitions.iter() {
        if log_source != schema.name {
            continue;
        }
        if let Some(pattern) = &schema.pattern{
            if let Some(captures) = pattern.captures(event) {
                let mut extracted_fields = Vec::new();
                
                // With named capture groups, you can iterate over the field names
        for field_name in schema.field_mappings.iter() {
            if let Some(value) = captures.name(field_name) {
                extracted_fields.push(value.as_str().to_string());
            }
        }
                
                return Some((schema.name.clone(), extracted_fields));
            }
        }
        
    }
    
    None // No matching schema found
}