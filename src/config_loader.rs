use std::{
    collections::BTreeMap,
    env as std_env,
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow};
use clap::{Error, Parser, error::ErrorKind};
#[cfg(test)]
use once_cell::sync::Lazy;
use serde::Deserialize;
use toml::Value;

use crate::cli::Cli;

/// Name of the default config file that will be auto-discovered in the
/// working directory when present.
const DEFAULT_CONFIG_FILENAME: &str = "parseable.toml";

/// Known storage subcommands supported by Parseable.
const STORAGE_SUBCOMMANDS: &[&str] = &["local-store", "s3-store", "blob-store", "gcs-store"];

/// Public entry point that loads the CLI after applying config-driven
/// environment overrides.
pub fn parse_cli_with_config() -> Cli {
    let raw_args: Vec<OsString> = std_env::args_os().collect();
    let adjusted_args = apply_config(raw_args)
        .unwrap_or_else(|err| Error::raw(ErrorKind::Io, err.to_string()).exit());

    Cli::parse_from(adjusted_args)
}

#[derive(Debug, Deserialize, Default)]
struct FileConfig {
    storage: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, Value>,
    #[serde(flatten)]
    #[serde(default)]
    inline_env: BTreeMap<String, Value>,
}

fn set_env_var<K: AsRef<OsStr>, V: AsRef<OsStr>>(key: K, value: V) {
    // SAFETY: std::env marks mutations as unsafe because concurrent writes can
    // lead to data races. We invoke these helpers before worker threads start
    // (or under a test mutex), matching std's documented safety guarantee.
    unsafe { std_env::set_var(key, value) }
}

#[cfg(test)]
fn remove_env_var<K: AsRef<OsStr>>(key: K) {
    unsafe { std_env::remove_var(key) }
}

/// Ensures our tests do not fight over global environment state.
#[cfg(test)]
static TEST_ENV_GUARD: Lazy<std::sync::Mutex<()>> = Lazy::new(|| std::sync::Mutex::new(()));

fn apply_config(mut args: Vec<OsString>) -> Result<Vec<OsString>> {
    let Some((config_path, source)) = locate_config_file(&args)? else {
        return Ok(args);
    };

    let contents = fs::read_to_string(&config_path).with_context(|| {
        format!(
            "Failed to read config file `{}` (source: {:?})",
            config_path.display(),
            source,
        )
    })?;

    let FileConfig {
        storage,
        mut env,
        inline_env,
    } = toml::from_str::<FileConfig>(&contents).with_context(|| {
        format!(
            "Failed to parse config file `{}` (source: {:?})",
            config_path.display(),
            source,
        )
    })?;

    env.extend(inline_env);
    apply_env_overrides(&config_path, env);

    // Record the resolved config path so it shows up in CLI help / telemetry.
    if std_env::var_os("P_CONFIG_FILE").is_none() {
        set_env_var("P_CONFIG_FILE", &config_path);
    }

    if should_inject_storage(&args) {
        if let Some(storage_name) = storage {
            args.push(OsString::from(storage_name));
        } else {
            return Err(anyhow!(
                "No storage backend provided via CLI arguments or `storage` key in `{}`",
                config_path.display()
            ));
        }
    }

    Ok(args)
}

#[derive(Debug, Clone, Copy)]
enum ConfigSource {
    Cli,
    Env,
    Default,
}
fn locate_config_file(args: &[OsString]) -> Result<Option<(PathBuf, ConfigSource)>> {
    if let Some(path) = config_from_args(args)? {
        return Ok(Some((path, ConfigSource::Cli)));
    }

    if let Some(env_path) = std_env::var_os("P_CONFIG_FILE") {
        let path = PathBuf::from(env_path);
        return Ok(Some((path, ConfigSource::Env)));
    }

    let default_path = PathBuf::from(DEFAULT_CONFIG_FILENAME);
    if default_path.is_file() {
        return Ok(Some((default_path, ConfigSource::Default)));
    }

    Ok(None)
}

fn config_from_args(args: &[OsString]) -> Result<Option<PathBuf>> {
    let mut iter = args.iter();
    // skip binary name
    iter.next();

    while let Some(raw_arg) = iter.next() {
        let arg = raw_arg.to_string_lossy();
        if arg == "--config-file" || arg == "-c" {
            let value = iter
                .next()
                .ok_or_else(|| anyhow!("`{arg}` expects a file path to follow"))?;
            return Ok(Some(PathBuf::from(value)));
        }

        if let Some(path) = arg.strip_prefix("--config-file=") {
            return Ok(Some(PathBuf::from(path)));
        }

        if let Some(path) = arg.strip_prefix("-c=") {
            return Ok(Some(PathBuf::from(path)));
        }
    }

    Ok(None)
}

fn apply_env_overrides(config_path: &Path, env_map: BTreeMap<String, Value>) {
    for (key, value) in env_map {
        if !looks_like_env_var(&key) {
            eprintln!(
                "Warning: Ignoring key `{}` in `{}` because it does not look like an env variable",
                key,
                config_path.display()
            );
            continue;
        }
        if std_env::var_os(&key).is_some() {
            continue;
        }
        match value_to_env_string(value) {
            Some(serialized) => set_env_var(&key, serialized),
            None => eprintln!(
                "Warning: Ignoring key `{}` in `{}` because nested tables are not supported",
                key,
                config_path.display()
            ),
        }
    }
}

fn value_to_env_string(value: Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s),
        Value::Integer(i) => Some(i.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Boolean(b) => Some(b.to_string()),
        Value::Datetime(dt) => Some(dt.to_string()),
        Value::Array(values) => Some(Value::Array(values).to_string()),
        Value::Table(_) => None,
    }
}

fn should_inject_storage(args: &[OsString]) -> bool {
    !has_storage_subcommand(args) && !is_help_or_version_call(args)
}

fn has_storage_subcommand(args: &[OsString]) -> bool {
    args.iter().skip(1).any(|arg| {
        let value = arg.to_string_lossy();
        STORAGE_SUBCOMMANDS.contains(&value.as_ref())
    })
}

fn is_help_or_version_call(args: &[OsString]) -> bool {
    args.iter().skip(1).any(|arg| {
        matches!(
            arg.to_string_lossy().as_ref(),
            "-h" | "--help" | "help" | "-V" | "--version" | "version"
        )
    })
}

fn looks_like_env_var(key: &str) -> bool {
    !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{ffi::OsString, fs::File, io::Write};

    fn build_config(contents: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut file = File::create(&path).unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        set_env_var("P_CONFIG_FILE", &path);
        dir
    }

    #[test]
    fn config_sets_env_and_storage() {
        let _guard = TEST_ENV_GUARD.lock().unwrap();
        remove_env_var("P_USERNAME");

        let dir = build_config(
            r#"
            storage = "local-store"
            P_USERNAME = "alice"
            "#,
        );

        let args = vec![OsString::from("parseable")];
        let updated = apply_config(args).unwrap();
        assert_eq!(updated.len(), 2);
        assert_eq!(updated[1], OsString::from("local-store"));
        assert_eq!(std_env::var("P_USERNAME").unwrap(), "alice");

        drop(dir);
        remove_env_var("P_CONFIG_FILE");
        remove_env_var("P_USERNAME");
    }

    #[test]
    fn cli_subcommand_not_overridden() {
        let _guard = TEST_ENV_GUARD.lock().unwrap();
        remove_env_var("P_CONFIG_FILE");
        let dir = build_config(
            r#"
            storage = "local-store"
            P_USERNAME = "bob"
            "#,
        );

        let args = vec![OsString::from("parseable"), OsString::from("s3-store")];
        let updated = apply_config(args).unwrap();
        assert_eq!(updated[1], OsString::from("s3-store"));
        assert!(std_env::var("P_USERNAME").is_ok());

        drop(dir);
        remove_env_var("P_CONFIG_FILE");
    }
}
