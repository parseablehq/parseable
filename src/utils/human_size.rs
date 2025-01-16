use std::str::FromStr;

use human_size::{multiples, SpecificSize};
use serde::{de, Deserialize, Deserializer, Serializer};

// Function to convert human-readable size to bytes (already provided)
pub fn human_size_to_bytes(s: &str) -> Result<u64, String> {
    fn parse_and_map<T: human_size::Multiple>(s: &str) -> Result<u64, human_size::ParsingError> {
        SpecificSize::<T>::from_str(s).map(|x| x.to_bytes())
    }

    let size = parse_and_map::<multiples::Mebibyte>(s)
        .or(parse_and_map::<multiples::Megabyte>(s))
        .or(parse_and_map::<multiples::Gigibyte>(s))
        .or(parse_and_map::<multiples::Gigabyte>(s))
        .or(parse_and_map::<multiples::Tebibyte>(s))
        .or(parse_and_map::<multiples::Terabyte>(s))
        .map_err(|_| "Could not parse given size".to_string())?;
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
        format!("{} B", bytes)
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
