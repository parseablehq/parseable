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
use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum Mode {
    Query,
    Ingest,
    #[default]
    All,
}

#[derive(Debug, thiserror::Error)]
#[error("Starting Standalone Mode is not permitted when Distributed Mode is enabled. Please restart the server with Distributed Mode enabled.")]
pub struct StandaloneWithDistributed;

impl Mode {
    // An instance is not allowed
    pub fn standalone_after_distributed(&self) -> Result<(), StandaloneWithDistributed> {
        if *self == Mode::Query {
            return Err(StandaloneWithDistributed);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    #[default]
    Lz4Raw,
    Zstd,
}

impl From<Compression> for parquet::basic::Compression {
    fn from(value: Compression) -> Self {
        match value {
            Compression::Uncompressed => parquet::basic::Compression::UNCOMPRESSED,
            Compression::Snappy => parquet::basic::Compression::SNAPPY,
            Compression::Gzip => parquet::basic::Compression::GZIP(GzipLevel::default()),
            Compression::Lzo => parquet::basic::Compression::LZO,
            Compression::Brotli => parquet::basic::Compression::BROTLI(BrotliLevel::default()),
            Compression::Lz4 => parquet::basic::Compression::LZ4,
            Compression::Lz4Raw => parquet::basic::Compression::LZ4_RAW,
            Compression::Zstd => parquet::basic::Compression::ZSTD(ZstdLevel::default()),
        }
    }
}

pub mod validation {
    use std::{
        env, io,
        net::ToSocketAddrs,
        path::{Path, PathBuf},
    };

    use path_clean::PathClean;

    #[cfg(any(
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64")
    ))]
    use crate::kafka::SslProtocol;

    use super::{Compression, Mode};

    pub fn file_path(s: &str) -> Result<PathBuf, String> {
        if s.is_empty() {
            return Err("empty path".to_owned());
        }

        let path = PathBuf::from(s);

        if !path.is_file() {
            return Err("path specified does not point to an accessible file".to_string());
        }

        Ok(path)
    }
    pub fn absolute_path(path: impl AsRef<Path>) -> io::Result<PathBuf> {
        let path = path.as_ref();

        let absolute_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            env::current_dir()?.join(path)
        }
        .clean();

        Ok(absolute_path)
    }

    pub fn canonicalize_path(s: &str) -> Result<PathBuf, String> {
        let path = PathBuf::from(s);
        Ok(absolute_path(path).unwrap())
    }

    pub fn socket_addr(s: &str) -> Result<String, String> {
        s.to_socket_addrs()
            .is_ok()
            .then_some(s.to_string())
            .ok_or_else(|| "Socket Address for server is invalid".to_string())
    }

    pub fn url(s: &str) -> Result<url::Url, String> {
        url::Url::parse(s).map_err(|_| "Invalid URL provided".to_string())
    }

    #[cfg(any(
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64")
    ))]
    pub fn kafka_security_protocol(s: &str) -> Result<SslProtocol, String> {
        match s {
            "plaintext" => Ok(SslProtocol::Plaintext),
            "ssl" => Ok(SslProtocol::Ssl),
            "sasl_plaintext" => Ok(SslProtocol::SaslPlaintext),
            "sasl_ssl" => Ok(SslProtocol::SaslSsl),
            _ => Err("Invalid Kafka Security Protocol provided".to_string()),
        }
    }

    pub fn mode(s: &str) -> Result<Mode, String> {
        match s {
            "query" => Ok(Mode::Query),
            "ingest" => Ok(Mode::Ingest),
            "all" => Ok(Mode::All),
            _ => Err("Invalid MODE provided".to_string()),
        }
    }

    pub fn compression(s: &str) -> Result<Compression, String> {
        match s {
            "uncompressed" => Ok(Compression::Uncompressed),
            "snappy" => Ok(Compression::Snappy),
            "gzip" => Ok(Compression::Gzip),
            "lzo" => Ok(Compression::Lzo),
            "brotli" => Ok(Compression::Brotli),
            "lz4" => Ok(Compression::Lz4),
            "lz4_raw" => Ok(Compression::Lz4Raw),
            "zstd" => Ok(Compression::Zstd),
            _ => Err("Invalid COMPRESSION provided".to_string()),
        }
    }

    pub fn validate_disk_usage(max_disk_usage: &str) -> Result<f64, String> {
        if let Ok(max_disk_usage) = max_disk_usage.parse::<f64>() {
            if (0.0..=100.0).contains(&max_disk_usage) {
                Ok(max_disk_usage)
            } else {
                Err("Invalid value for max disk usage. It should be between 0 and 100".to_string())
            }
        } else {
            Err("Invalid value for max disk usage. It should be given as 90.0 for 90%".to_string())
        }
    }
}
