use std::str::FromStr;

use human_size::{Any, SpecificSize};
use serde::{Deserialize, Deserializer, Serializer, de};

#[derive(Debug, thiserror::Error)]
enum ParsingError {
    #[error("Expected 'X' | 'X Bytes', but error: {0}")]
    Int(#[from] std::num::ParseIntError),
    #[error("Could not parse given string as human size, erro: {0}")]
    HumanSize(#[from] human_size::ParsingError),
}

// Function to convert human-readable size to bytes (already provided)
// NOTE: consider number values as byte count, e.g. "1234" is 1234 bytes.
fn human_size_to_bytes(s: &str) -> Result<u64, ParsingError> {
    let s = s.trim();
    if let Some(s) = s.strip_suffix("Bytes") {
        let size: u64 = s.trim().parse()?;
        return Ok(size);
    } else if let Ok(size) = s.parse() {
        return Ok(size);
    }

    fn parse_and_map<T: human_size::Multiple>(s: &str) -> Result<u64, human_size::ParsingError> {
        SpecificSize::<T>::from_str(s).map(|x| x.to_bytes())
    }
    let size = parse_and_map::<Any>(s)?;

    Ok(size)
}

// Function to convert bytes to human-readable size (already provided)
pub fn bytes_to_human_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    const TIB: u64 = GIB * 1024;
    const PIB: u64 = TIB * 1024;

    if bytes < KIB {
        format!("{bytes} B")
    } else if bytes < MIB {
        format!("{:.2} KB", bytes as f64 / KIB as f64)
    } else if bytes < GIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes < TIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes < PIB {
        format!("{:.2} TiB", bytes as f64 / TIB as f64)
    } else {
        format!("{:.2} PiB", bytes as f64 / PIB as f64)
    }
}

pub fn serialize<S>(bytes: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // let human_readable = bytes_to_human_size(*bytes);
    // NOTE: frontend expects the size in bytes
    let human_readable = format!("{bytes} Bytes");
    serializer.serialize_str(&human_readable)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    human_size_to_bytes(&s).map_err(de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_numeric_input_without_unit() {
        assert_eq!(human_size_to_bytes("1234").unwrap(), 1234);
    }

    #[test]
    fn parse_bytes_string_to_bytes() {
        assert_eq!(human_size_to_bytes("1234 Bytes").unwrap(), 1234);
    }

    #[test]
    fn handle_empty_string_input() {
        assert!(matches!(
            human_size_to_bytes(""),
            Err(ParsingError::HumanSize(_))
        ));
    }

    #[test]
    fn handle_byte_string_input_without_value() {
        assert!(matches!(
            human_size_to_bytes("Bytes"),
            Err(ParsingError::Int(_))
        ));
    }

    #[test]
    fn convert_mebibyte_string_to_bytes() {
        assert_eq!(human_size_to_bytes("1 MiB").unwrap(), 1048576);
    }

    #[test]
    fn parse_gigabyte_string_input() {
        assert_eq!(human_size_to_bytes("1 GB").unwrap(), 1_000_000_000);
    }
}
