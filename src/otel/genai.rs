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

use serde_json::{Map, Number, Value};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::LazyLock;

/// Known GenAI fields that should be stored as integers (OTel sends IntValue as string).
/// These fields need type coercion from String -> i64 when dataset tag is agent-observability.
pub const GENAI_INT_FIELDS: &[&str] = &[
    "gen_ai.usage.input_tokens",
    "gen_ai.usage.output_tokens",
    "gen_ai.usage.cache_creation.input_tokens",
    "gen_ai.usage.cache_read.input_tokens",
    "gen_ai.request.max_tokens",
    "gen_ai.request.seed",
];

/// Known GenAI fields that should be stored as floats.
pub const GENAI_FLOAT_FIELDS: &[&str] = &[
    "gen_ai.request.temperature",
    "gen_ai.request.top_p",
    "gen_ai.request.top_k",
    "gen_ai.request.frequency_penalty",
    "gen_ai.request.presence_penalty",
];

/// Complete list of known GenAI fields with their expected types.
/// This is used for auto-schema creation when a stream with
/// `X-P-Dataset-Tag: agent-observability` is first set up.
///
/// Format: (field_name, arrow_type_string)
/// Types: "Utf8" = String, "Int64" = integer, "Float64" = float
pub const GENAI_KNOWN_FIELD_LIST: &[(&str, &str)] = &[
    // GenAI Identity & Operation
    ("gen_ai.operation.name", "Utf8"),
    ("gen_ai.provider.name", "Utf8"),
    ("gen_ai.conversation.id", "Utf8"),
    // Model Request
    ("gen_ai.request.model", "Utf8"),
    ("gen_ai.request.temperature", "Float64"),
    ("gen_ai.request.top_p", "Float64"),
    ("gen_ai.request.top_k", "Float64"),
    ("gen_ai.request.max_tokens", "Int64"),
    ("gen_ai.request.seed", "Int64"),
    ("gen_ai.request.frequency_penalty", "Float64"),
    ("gen_ai.request.presence_penalty", "Float64"),
    ("gen_ai.request.stop_sequences", "Utf8"),
    // Model Response
    ("gen_ai.response.id", "Utf8"),
    ("gen_ai.response.model", "Utf8"),
    ("gen_ai.response.finish_reasons", "Utf8"),
    // Token Usage
    ("gen_ai.usage.input_tokens", "Int64"),
    ("gen_ai.usage.output_tokens", "Int64"),
    ("gen_ai.usage.cache_creation.input_tokens", "Int64"),
    ("gen_ai.usage.cache_read.input_tokens", "Int64"),
    // Agent
    ("gen_ai.agent.name", "Utf8"),
    ("gen_ai.agent.id", "Utf8"),
    ("gen_ai.agent.description", "Utf8"),
    // Tool Execution
    ("gen_ai.tool.name", "Utf8"),
    ("gen_ai.tool.type", "Utf8"),
    ("gen_ai.tool.call.id", "Utf8"),
    ("gen_ai.tool.call.arguments", "Utf8"),
    ("gen_ai.tool.call.result", "Utf8"),
    // Parseable-enriched columns (computed at ingest time)
    ("p_genai_cost_usd", "Float64"),
    ("p_genai_tokens_total", "Int64"),
    ("p_genai_tokens_per_sec", "Float64"),
    ("p_genai_duration_ms", "Float64"),
];

/// Per-token pricing: (input_price_per_token, output_price_per_token) in USD.
/// Prices are per-token (not per 1K or 1M tokens).
/// Updated with major model pricing as of early 2026.
static DEFAULT_PRICING: LazyLock<HashMap<&'static str, (f64, f64)>> = LazyLock::new(|| {
    HashMap::from([
        // OpenAI
        ("gpt-4o", (2.5e-6, 10.0e-6)),
        ("gpt-4o-2024-11-20", (2.5e-6, 10.0e-6)),
        ("gpt-4o-2024-08-06", (2.5e-6, 10.0e-6)),
        ("gpt-4o-mini", (0.15e-6, 0.6e-6)),
        ("gpt-4o-mini-2024-07-18", (0.15e-6, 0.6e-6)),
        ("gpt-4-turbo", (10.0e-6, 30.0e-6)),
        ("gpt-4-turbo-2024-04-09", (10.0e-6, 30.0e-6)),
        ("gpt-4", (30.0e-6, 60.0e-6)),
        ("gpt-3.5-turbo", (0.5e-6, 1.5e-6)),
        ("o1", (15.0e-6, 60.0e-6)),
        ("o1-mini", (3.0e-6, 12.0e-6)),
        ("o1-preview", (15.0e-6, 60.0e-6)),
        ("o3-mini", (1.1e-6, 4.4e-6)),
        // Anthropic
        ("claude-sonnet-4-20250514", (3.0e-6, 15.0e-6)),
        ("claude-3-5-sonnet-20241022", (3.0e-6, 15.0e-6)),
        ("claude-3-5-sonnet-20240620", (3.0e-6, 15.0e-6)),
        ("claude-3-5-haiku-20241022", (0.8e-6, 4.0e-6)),
        ("claude-3-opus-20240229", (15.0e-6, 75.0e-6)),
        ("claude-3-haiku-20240307", (0.25e-6, 1.25e-6)),
        ("claude-opus-4-20250514", (15.0e-6, 75.0e-6)),
        // Google
        ("gemini-1.5-pro", (1.25e-6, 5.0e-6)),
        ("gemini-1.5-flash", (0.075e-6, 0.3e-6)),
        ("gemini-2.0-flash", (0.1e-6, 0.4e-6)),
        ("gemini-2.0-flash-lite", (0.075e-6, 0.3e-6)),
        // Mistral
        ("mistral-large-latest", (2.0e-6, 6.0e-6)),
        ("mistral-small-latest", (0.1e-6, 0.3e-6)),
        ("codestral-latest", (0.3e-6, 0.9e-6)),
        // Cohere
        ("command-r-plus", (2.5e-6, 10.0e-6)),
        ("command-r", (0.15e-6, 0.6e-6)),
        // Meta (via providers)
        ("llama-3.1-405b", (3.0e-6, 3.0e-6)),
        ("llama-3.1-70b", (0.35e-6, 0.4e-6)),
        ("llama-3.1-8b", (0.05e-6, 0.08e-6)),
        // Groq
        ("llama3-70b-8192", (0.59e-6, 0.79e-6)),
        ("llama3-8b-8192", (0.05e-6, 0.08e-6)),
        ("mixtral-8x7b-32768", (0.24e-6, 0.24e-6)),
    ])
});

/// Lazily loads user pricing overrides from config file, merged with defaults.
static PRICING_TABLE: LazyLock<HashMap<String, (f64, f64)>> = LazyLock::new(|| {
    let mut table: HashMap<String, (f64, f64)> = DEFAULT_PRICING
        .iter()
        .map(|(&k, &v)| (k.to_string(), v))
        .collect();

    // Try to load user overrides from config directory
    if let Some(config_path) = find_pricing_config() {
        match load_pricing_overrides(&config_path) {
            Ok(overrides) => {
                tracing::info!(
                    "Loaded {} GenAI pricing overrides from {}",
                    overrides.len(),
                    config_path
                );
                table.extend(overrides);
            }
            Err(e) => {
                tracing::warn!("Failed to load GenAI pricing config from {}: {}", config_path, e);
            }
        }
    }

    table
});

/// Search for pricing config file in standard locations.
fn find_pricing_config() -> Option<String> {
    let candidates = [
        // Parseable config directory (set by user)
        std::env::var("PARSEABLE_CONFIG_DIR")
            .map(|d| format!("{}/genai-pricing.json", d))
            .ok(),
        // Current working directory
        Some("genai-pricing.json".to_string()),
        // Home directory
        std::env::var("HOME")
            .map(|h| format!("{}/.parseable/genai-pricing.json", h))
            .ok(),
    ];

    candidates.into_iter().flatten().find(|p| Path::new(p).exists())
}

/// Load pricing overrides from a JSON file.
/// Expected format: { "model-name": { "input": 0.000003, "output": 0.000015 } }
fn load_pricing_overrides(path: &str) -> Result<HashMap<String, (f64, f64)>, String> {
    let contents = fs::read_to_string(path).map_err(|e| e.to_string())?;
    let parsed: HashMap<String, serde_json::Value> =
        serde_json::from_str(&contents).map_err(|e| e.to_string())?;

    let mut overrides = HashMap::new();
    for (model, pricing) in parsed {
        let input = pricing
            .get("input")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| format!("Missing 'input' price for model '{}'", model))?;
        let output = pricing
            .get("output")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| format!("Missing 'output' price for model '{}'", model))?;
        overrides.insert(model, (input, output));
    }
    Ok(overrides)
}

/// Look up pricing for a model name. Tries exact match first, then prefix match
/// (e.g., "gpt-4o-2024-11-20" matches "gpt-4o" as fallback).
fn lookup_pricing(model: &str) -> Option<(f64, f64)> {
    // Exact match
    if let Some(&pricing) = PRICING_TABLE.get(model) {
        return Some(pricing);
    }

    // Prefix match: find the longest matching prefix
    let mut best_match: Option<(&str, (f64, f64))> = None;
    for (key, &pricing) in PRICING_TABLE.iter() {
        if model.starts_with(key.as_str()) {
            match best_match {
                Some((best_key, _)) if key.len() > best_key.len() => {
                    best_match = Some((key, pricing));
                }
                None => {
                    best_match = Some((key, pricing));
                }
                _ => {}
            }
        }
    }
    best_match.map(|(_, pricing)| pricing)
}

/// Apply type coercion for known GenAI fields.
/// OTel's `IntValue` is serialized as strings by `collect_json_from_value`.
/// This function converts string representations of integers and floats
/// to their proper JSON numeric types for known GenAI fields.
pub fn coerce_genai_field_types(record: &mut Map<String, Value>) {
    for &field in GENAI_INT_FIELDS {
        if let Some(Value::String(s)) = record.get(field) {
            if let Ok(n) = s.parse::<i64>() {
                record.insert(field.to_string(), Value::Number(Number::from(n)));
            }
        }
    }

    for &field in GENAI_FLOAT_FIELDS {
        if let Some(Value::String(s)) = record.get(field) {
            if let Ok(f) = s.parse::<f64>() {
                if let Some(n) = Number::from_f64(f) {
                    record.insert(field.to_string(), Value::Number(n));
                }
            }
        }
    }
}

/// Enrich a GenAI span record with Parseable-computed fields:
/// - `p_genai_cost_usd`: estimated cost based on model pricing table
/// - `p_genai_tokens_total`: sum of input + output tokens
/// - `p_genai_tokens_per_sec`: output throughput (tokens/sec)
/// - `p_genai_duration_ms`: span duration in milliseconds
///
/// This function should be called after `coerce_genai_field_types`.
pub fn enrich_genai_record(record: &mut Map<String, Value>) {
    // Extract token counts
    let input_tokens = record
        .get("gen_ai.usage.input_tokens")
        .and_then(|v| v.as_i64());
    let output_tokens = record
        .get("gen_ai.usage.output_tokens")
        .and_then(|v| v.as_i64());

    // p_genai_tokens_total
    if let (Some(inp), Some(out)) = (input_tokens, output_tokens) {
        record.insert(
            "p_genai_tokens_total".to_string(),
            Value::Number(Number::from(inp + out)),
        );
    }

    // p_genai_duration_ms — computed from span_duration_ns
    let duration_ns = record
        .get("span_duration_ns")
        .and_then(|v| v.as_u64());

    if let Some(ns) = duration_ns {
        let duration_ms = ns as f64 / 1_000_000.0;
        if let Some(n) = Number::from_f64(duration_ms) {
            record.insert("p_genai_duration_ms".to_string(), Value::Number(n));
        }

        // p_genai_tokens_per_sec
        if let Some(out) = output_tokens {
            let duration_sec = ns as f64 / 1_000_000_000.0;
            if duration_sec > 0.0 {
                let tokens_per_sec = out as f64 / duration_sec;
                if let Some(n) = Number::from_f64(tokens_per_sec) {
                    record.insert("p_genai_tokens_per_sec".to_string(), Value::Number(n));
                }
            }
        }
    }

    // p_genai_cost_usd — look up model pricing
    let model = record
        .get("gen_ai.response.model")
        .or_else(|| record.get("gen_ai.request.model"))
        .and_then(|v| v.as_str());

    if let Some(model_name) = model {
        if let (Some(inp), Some(out)) = (input_tokens, output_tokens) {
            if let Some((input_price, output_price)) = lookup_pricing(model_name) {
                let cost = (inp as f64 * input_price) + (out as f64 * output_price);
                if let Some(n) = Number::from_f64(cost) {
                    record.insert("p_genai_cost_usd".to_string(), Value::Number(n));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_coerce_int_fields() {
        let mut record = Map::new();
        record.insert(
            "gen_ai.usage.input_tokens".to_string(),
            Value::String("1250".to_string()),
        );
        record.insert(
            "gen_ai.usage.output_tokens".to_string(),
            Value::String("350".to_string()),
        );
        record.insert(
            "gen_ai.request.max_tokens".to_string(),
            Value::String("4096".to_string()),
        );

        coerce_genai_field_types(&mut record);

        assert_eq!(
            record.get("gen_ai.usage.input_tokens").unwrap(),
            &json!(1250)
        );
        assert_eq!(
            record.get("gen_ai.usage.output_tokens").unwrap(),
            &json!(350)
        );
        assert_eq!(
            record.get("gen_ai.request.max_tokens").unwrap(),
            &json!(4096)
        );
    }

    #[test]
    fn test_coerce_float_fields() {
        let mut record = Map::new();
        record.insert(
            "gen_ai.request.temperature".to_string(),
            Value::String("0.7".to_string()),
        );
        record.insert(
            "gen_ai.request.top_p".to_string(),
            Value::String("0.95".to_string()),
        );

        coerce_genai_field_types(&mut record);

        assert_eq!(
            record.get("gen_ai.request.temperature").unwrap(),
            &json!(0.7)
        );
        assert_eq!(
            record.get("gen_ai.request.top_p").unwrap(),
            &json!(0.95)
        );
    }

    #[test]
    fn test_coerce_skips_non_string_values() {
        let mut record = Map::new();
        // Already a number — should not be changed
        record.insert(
            "gen_ai.usage.input_tokens".to_string(),
            Value::Number(Number::from(500)),
        );

        coerce_genai_field_types(&mut record);

        assert_eq!(
            record.get("gen_ai.usage.input_tokens").unwrap(),
            &json!(500)
        );
    }

    #[test]
    fn test_coerce_handles_invalid_strings() {
        let mut record = Map::new();
        record.insert(
            "gen_ai.usage.input_tokens".to_string(),
            Value::String("not-a-number".to_string()),
        );

        coerce_genai_field_types(&mut record);

        // Should remain as string since it can't be parsed
        assert_eq!(
            record.get("gen_ai.usage.input_tokens").unwrap(),
            &Value::String("not-a-number".to_string())
        );
    }

    #[test]
    fn test_enrich_basic() {
        let mut record = Map::new();
        record.insert(
            "gen_ai.usage.input_tokens".to_string(),
            Value::Number(Number::from(1000)),
        );
        record.insert(
            "gen_ai.usage.output_tokens".to_string(),
            Value::Number(Number::from(500)),
        );
        record.insert(
            "span_duration_ns".to_string(),
            Value::Number(Number::from(2_000_000_000u64)), // 2 seconds
        );
        record.insert(
            "gen_ai.request.model".to_string(),
            Value::String("gpt-4o".to_string()),
        );

        enrich_genai_record(&mut record);

        // tokens total
        assert_eq!(
            record.get("p_genai_tokens_total").unwrap(),
            &json!(1500)
        );

        // duration ms
        assert_eq!(
            record.get("p_genai_duration_ms").unwrap(),
            &json!(2000.0)
        );

        // tokens per sec: 500 / 2.0 = 250.0
        assert_eq!(
            record.get("p_genai_tokens_per_sec").unwrap(),
            &json!(250.0)
        );

        // cost: (1000 * 2.5e-6) + (500 * 10.0e-6) = 0.0025 + 0.005 = 0.0075
        let cost = record
            .get("p_genai_cost_usd")
            .unwrap()
            .as_f64()
            .unwrap();
        assert!((cost - 0.0075).abs() < 1e-10);
    }

    #[test]
    fn test_enrich_uses_response_model_over_request_model() {
        let mut record = Map::new();
        record.insert(
            "gen_ai.usage.input_tokens".to_string(),
            Value::Number(Number::from(100)),
        );
        record.insert(
            "gen_ai.usage.output_tokens".to_string(),
            Value::Number(Number::from(50)),
        );
        record.insert(
            "span_duration_ns".to_string(),
            Value::Number(Number::from(1_000_000_000u64)),
        );
        record.insert(
            "gen_ai.request.model".to_string(),
            Value::String("gpt-4".to_string()),
        );
        record.insert(
            "gen_ai.response.model".to_string(),
            Value::String("gpt-4o-mini".to_string()),
        );

        enrich_genai_record(&mut record);

        // Should use gpt-4o-mini pricing (cheaper)
        let cost = record.get("p_genai_cost_usd").unwrap().as_f64().unwrap();
        // gpt-4o-mini: (100 * 0.15e-6) + (50 * 0.6e-6) = 0.000015 + 0.00003 = 0.000045
        assert!((cost - 0.000045).abs() < 1e-10);
    }

    #[test]
    fn test_enrich_unknown_model_no_cost() {
        let mut record = Map::new();
        record.insert(
            "gen_ai.usage.input_tokens".to_string(),
            Value::Number(Number::from(100)),
        );
        record.insert(
            "gen_ai.usage.output_tokens".to_string(),
            Value::Number(Number::from(50)),
        );
        record.insert(
            "span_duration_ns".to_string(),
            Value::Number(Number::from(1_000_000_000u64)),
        );
        record.insert(
            "gen_ai.request.model".to_string(),
            Value::String("my-custom-finetuned-model".to_string()),
        );

        enrich_genai_record(&mut record);

        // Should still have tokens_total and duration, but no cost
        assert!(record.contains_key("p_genai_tokens_total"));
        assert!(record.contains_key("p_genai_duration_ms"));
        assert!(!record.contains_key("p_genai_cost_usd"));
    }

    #[test]
    fn test_enrich_missing_tokens_no_enrichment() {
        let mut record = Map::new();
        record.insert(
            "span_duration_ns".to_string(),
            Value::Number(Number::from(1_000_000_000u64)),
        );
        record.insert(
            "gen_ai.request.model".to_string(),
            Value::String("gpt-4o".to_string()),
        );

        enrich_genai_record(&mut record);

        assert!(!record.contains_key("p_genai_tokens_total"));
        assert!(!record.contains_key("p_genai_cost_usd"));
        // Duration should still be computed
        assert!(record.contains_key("p_genai_duration_ms"));
    }

    #[test]
    fn test_lookup_pricing_prefix_match() {
        // "gpt-4o-2024-11-20" should match "gpt-4o" via prefix
        let pricing = lookup_pricing("gpt-4o-some-future-version");
        assert!(pricing.is_some());
        let (input, output) = pricing.unwrap();
        assert!((input - 2.5e-6).abs() < 1e-12);
        assert!((output - 10.0e-6).abs() < 1e-12);
    }

    #[test]
    fn test_genai_known_field_list_count() {
        // 27 GenAI fields + 4 Parseable-enriched = 31 total
        assert_eq!(GENAI_KNOWN_FIELD_LIST.len(), 31);
    }
}
