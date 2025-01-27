use std::str::FromStr;

use human_size::{Any, SpecificSize};
use serde::{de, Deserialize, Deserializer, Serializer};

// Function to convert human-readable size to bytes (already provided)
// NOTE: consider number values as byte count, e.g. "1234" is 1234 bytes.
pub fn human_size_to_bytes(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if let Some(s) = s.strip_suffix("Bytes") {
        let size = s.trim().parse().expect("Suffix bytes implies byte count");
        return Ok(size);
    } else if let Ok(size) = s.parse() {
        return Ok(size);
    }

    fn parse_and_map<T: human_size::Multiple>(s: &str) -> Result<u64, human_size::ParsingError> {
        SpecificSize::<T>::from_str(s).map(|x| x.to_bytes())
    }

    let size = parse_and_map::<Any>(s).map_err(|_| "Could not parse given size".to_string())?;

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

#[cfg(test)]
mod tests {
    use crate::utils::human_size::human_size_to_bytes;

    #[test]
    fn parse_numeric_input_without_unit() {
        assert_eq!(human_size_to_bytes("1234"), Ok(1234));
    }

    #[test]
    fn parse_bytes_string_to_bytes() {
        assert_eq!(human_size_to_bytes("1234 Bytes"), Ok(1234));
    }

    #[test]
    fn handle_empty_string_input() {
        assert_eq!(
            human_size_to_bytes(""),
            Err("Could not parse given size".to_string())
        );
    }

    #[test]
    fn convert_mebibyte_string_to_bytes() {
        assert_eq!(human_size_to_bytes("1 MiB"), Ok(1048576));
    }

    #[test]
    fn parse_gigabyte_string_input() {
        assert_eq!(human_size_to_bytes("1 GB"), Ok(1_000_000_000));
    }
}
