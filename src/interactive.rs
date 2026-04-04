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

use std::io::{self, BufRead, IsTerminal, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

const ENV_FILE_NAME: &str = ".parseable.env";

/// Guard to avoid parsing `.parseable.env` more than once per process.
static ENV_FILE_LOADED: AtomicBool = AtomicBool::new(false);

/// A required or optional env var that the user may need to provide.
struct EnvPrompt {
    env_var: &'static str,
    display_name: &'static str,
    required: bool,
    is_secret: bool,
}

/// Checks for missing environment variables based on the subcommand
/// detected in `std::env::args()`. Covers storage-specific env vars
/// (S3, Azure Blob, GCS) as well as conditionally-required groups
/// like OIDC. If running in an interactive terminal and required env
/// vars are missing, prompts the user to enter them.
///
/// On startup, it first loads any previously saved values from `.parseable.env`.
/// After interactive collection, it saves all collected values back to that file
/// and prints `export` commands so the user can source them in their shell.
///
/// Must be called **before** `Cli::parse()` so that clap sees the values.
///
/// Returns a list of `(env_var, value)` pairs collected interactively.
/// The caller is responsible for persisting these to `.parseable.env`
/// only after clap validation succeeds (via [`save_collected_envs`]).
pub fn prompt_missing_envs() -> Vec<(String, String)> {
    // Bail out for help/version flags so clap can handle them directly.
    if is_help_or_version_request() {
        return vec![];
    }

    let subcommand = match detect_storage_subcommand() {
        Some(cmd) => cmd,
        None => return vec![],
    };

    // Load previously saved env vars from .parseable.env (if it exists).
    // This must run before get_env_prompts() because OIDC detection
    // checks which env vars are already set.
    load_env_file();

    let is_interactive = io::stdin().is_terminal();
    let mut collected: Vec<(String, String)> = Vec::new();

    let prompts = get_env_prompts(&subcommand);
    collect_prompts(&prompts, is_interactive, &subcommand, &mut collected);

    collected
}

/// Persists interactively-collected env vars to `.parseable.env`.
/// Should only be called after clap validation succeeds.
/// Persistence is best-effort — a read-only working directory will
/// produce a warning but not prevent the server from starting.
pub fn save_collected_envs(collected: &[(String, String)]) {
    if collected.is_empty() {
        return;
    }

    let pairs: Vec<(&str, String)> = collected
        .iter()
        .map(|(k, v)| (k.as_str(), v.clone()))
        .collect();

    match save_env_file(&pairs) {
        Ok(()) => {
            let env_path = env_file_path();
            println!();
            println!("  Configuration saved to {}", env_path.display());
            println!("  These values will be loaded automatically on next startup.");
            println!();
            println!("  To set these in your current shell, run:");
            println!("    source {}", env_path.display());
            println!();
        }
        Err(err) => {
            eprintln!("  Warning: could not persist interactive configuration: {err}");
            eprintln!("  The server will continue, but values won't be saved for next startup.");
        }
    }
}

/// Returns the path to the `.parseable.env` file in the current directory.
fn env_file_path() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(ENV_FILE_NAME)
}

/// Loads env vars from `.parseable.env` if it exists.
/// Format: KEY=VALUE per line, # comments and empty lines are skipped.
/// Safe to call multiple times — the file is only parsed once per process.
pub fn load_env_file() {
    if ENV_FILE_LOADED.swap(true, Ordering::Relaxed) {
        return;
    }
    let path = env_file_path();
    let file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(_) => return,
    };

    for line in io::BufReader::new(file).lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        let line = line.trim().to_string();

        // Skip comments and empty lines
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Handle export prefix: `export KEY=VALUE`
        let line = line.strip_prefix("export ").unwrap_or(&line).to_string();

        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            // Strip surrounding quotes and decode shell-escaped single quotes.
            // save_env_file writes: export KEY='val'\''ue' for values containing '
            // so after stripping outer quotes we must reverse the '\'' escape.
            let value = value
                .strip_prefix('\'')
                .and_then(|v| v.strip_suffix('\''))
                .or_else(|| value.strip_prefix('"').and_then(|v| v.strip_suffix('"')))
                .unwrap_or(value);
            let value = value.replace("'\\''", "'");
            // Only set if not already set in the environment (explicit env takes precedence)
            if std::env::var(key).is_err() {
                // SAFETY: Single-threaded startup, no other threads running.
                unsafe { std::env::set_var(key, &value) };
            }
        }
    }
}

/// Appends collected env vars to `.parseable.env`.
/// If the file already exists, new values are appended (avoiding duplicates).
/// Returns an error instead of panicking so callers can treat persistence
/// as best-effort.
fn save_env_file(collected: &[(&str, String)]) -> io::Result<()> {
    let path = env_file_path();

    // Read existing keys to avoid duplicates
    let existing_keys: std::collections::HashSet<String> = std::fs::File::open(&path)
        .ok()
        .map(|f| {
            io::BufReader::new(f)
                .lines()
                .map_while(Result::ok)
                .filter_map(|l| {
                    let l = l.trim().to_string();
                    if l.is_empty() || l.starts_with('#') {
                        return None;
                    }
                    let l = l.strip_prefix("export ").unwrap_or(&l).to_string();
                    l.split_once('=').map(|(k, _)| k.trim().to_string())
                })
                .collect()
        })
        .unwrap_or_default();

    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    opts.mode(0o600);
    let mut file = opts.open(&path)?;

    #[cfg(unix)]
    if let Err(e) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)) {
        eprintln!("  Warning: Could not set restrictive permissions on .parseable.env: {e}");
    }

    for (key, value) in collected {
        if existing_keys.contains(*key) {
            continue;
        }
        // Escape single quotes and wrap in single quotes for shell safety
        let escaped = value.replace('\'', "'\\''");
        writeln!(file, "export {key}='{escaped}'")?;
    }

    Ok(())
}

/// Returns true if the user passed a help or version flag anywhere in argv.
/// Covers: `-h`, `--help`, `-V`, `--version`, and subcommand-specific help
/// like `parseable s3-store --help`.
fn is_help_or_version_request() -> bool {
    std::env::args()
        .skip(1)
        .any(|arg| matches!(arg.as_str(), "-h" | "--help" | "-V" | "--version" | "help"))
}

/// Detects which storage subcommand was passed (e.g. "s3-store", "blob-store").
fn detect_storage_subcommand() -> Option<String> {
    let known = ["s3-store", "blob-store", "gcs-store", "local-store"];
    std::env::args()
        .skip(1)
        .find(|arg| known.contains(&arg.as_str()))
}

/// Returns the list of env var prompts for the given storage subcommand,
/// including any conditionally-required groups like OIDC and mode-specific vars.
fn get_env_prompts(subcommand: &str) -> Vec<EnvPrompt> {
    let mut prompts = get_storage_prompts(subcommand);
    prompts.extend(get_tls_prompts());
    prompts.extend(get_oidc_prompts());
    #[cfg(feature = "kafka")]
    prompts.extend(get_kafka_prompts());
    prompts
}

/// Detects the server mode from the `P_MODE` env var or the `--mode` CLI arg.
fn detect_mode() -> Option<String> {
    if let Ok(mode) = std::env::var("P_MODE") {
        let mode = mode.to_lowercase();
        if mode != "all" {
            return Some(mode);
        }
        return None;
    }

    let args: Vec<String> = std::env::args().collect();
    for (i, arg) in args.iter().enumerate() {
        if arg == "--mode" {
            return args.get(i + 1).map(|v| v.to_lowercase());
        }
        if let Some(value) = arg.strip_prefix("--mode=") {
            return Some(value.to_lowercase());
        }
    }

    None
}

/// Returns storage-specific env var prompts for the given subcommand.
fn get_storage_prompts(subcommand: &str) -> Vec<EnvPrompt> {
    match subcommand {
        "s3-store" => vec![
            EnvPrompt {
                env_var: "P_S3_URL",
                display_name: "S3 Endpoint URL",
                required: true,
                is_secret: false,
            },
            EnvPrompt {
                env_var: "P_S3_REGION",
                display_name: "S3 Region",
                required: true,
                is_secret: false,
            },
            EnvPrompt {
                env_var: "P_S3_BUCKET",
                display_name: "S3 Bucket Name",
                required: true,
                is_secret: false,
            },
            EnvPrompt {
                env_var: "P_S3_ACCESS_KEY",
                display_name: "S3 Access Key",
                required: false,
                is_secret: false,
            },
            EnvPrompt {
                env_var: "P_S3_SECRET_KEY",
                display_name: "S3 Secret Key",
                required: false,
                is_secret: true,
            },
        ],
        "blob-store" => vec![
            EnvPrompt {
                env_var: "P_AZR_URL",
                display_name: "Azure Blob Endpoint URL",
                required: true,
                is_secret: false,
            },
            EnvPrompt {
                env_var: "P_AZR_ACCOUNT",
                display_name: "Azure Storage Account",
                required: true,
                is_secret: false,
            },
            EnvPrompt {
                env_var: "P_AZR_CONTAINER",
                display_name: "Azure Container Name",
                required: true,
                is_secret: false,
            },
            EnvPrompt {
                env_var: "P_AZR_ACCESS_KEY",
                display_name: "Azure Access Key",
                required: false,
                is_secret: true,
            },
        ],
        "gcs-store" => vec![EnvPrompt {
            env_var: "P_GCS_BUCKET",
            display_name: "GCS Bucket Name",
            required: true,
            is_secret: false,
        }],
        _ => vec![],
    }
}

/// Returns TLS env var prompts if TLS is partially configured.
///
/// Both `P_TLS_CERT_PATH` and `P_TLS_KEY_PATH` are `Option<PathBuf>` so
/// clap won't fail if only one is set — but the server silently falls back
/// to HTTP, which is almost certainly not what the user intended. If either
/// is set, prompt for the other.
fn get_tls_prompts() -> Vec<EnvPrompt> {
    const TLS_ENVS: [(&str, &str); 2] = [
        ("P_TLS_CERT_PATH", "TLS Certificate Path"),
        ("P_TLS_KEY_PATH", "TLS Private Key Path"),
    ];

    let any_set = TLS_ENVS.iter().any(|(env, _)| std::env::var(env).is_ok());
    let all_set = TLS_ENVS.iter().all(|(env, _)| std::env::var(env).is_ok());

    if !any_set || all_set {
        return vec![];
    }

    TLS_ENVS
        .iter()
        .map(|(env_var, display_name)| EnvPrompt {
            env_var,
            display_name,
            required: true,
            is_secret: false,
        })
        .collect()
}

/// Returns OIDC env var prompts if OIDC is partially configured.
///
/// `OidcConfig` is flattened as `Option<OidcConfig>` in `Options` — clap
/// activates the entire group when *any* OIDC env var is provided, making
/// all three fields required. So if a user sets one but not the others,
/// the server fails. This function detects that partial state and returns
/// the OIDC vars as required prompts.
fn get_oidc_prompts() -> Vec<EnvPrompt> {
    const OIDC_ENVS: [(&str, &str, bool); 3] = [
        ("P_OIDC_CLIENT_ID", "OIDC Client ID", false),
        ("P_OIDC_CLIENT_SECRET", "OIDC Client Secret", true),
        ("P_OIDC_ISSUER", "OIDC Issuer URL", false),
    ];

    let any_set = OIDC_ENVS
        .iter()
        .any(|(env, _, _)| std::env::var(env).is_ok());

    if !any_set {
        return vec![];
    }

    OIDC_ENVS
        .iter()
        .map(|(env_var, display_name, is_secret)| EnvPrompt {
            env_var,
            display_name,
            required: true,
            is_secret: *is_secret,
        })
        .collect()
}

/// Returns Kafka env var prompts if Kafka is partially configured.
///
/// Kafka has layered dependencies:
/// - If any `P_KAFKA_*` env is set, `P_KAFKA_BOOTSTRAP_SERVERS` and
///   `P_KAFKA_CONSUMER_TOPICS` are required for the server to function.
/// - If security protocol is SSL or SASL_SSL, SSL cert paths are required.
/// - If security protocol is SASL_PLAINTEXT or SASL_SSL, SASL credentials
///   are required.
#[cfg(feature = "kafka")]
fn get_kafka_prompts() -> Vec<EnvPrompt> {
    // Check if any Kafka env var is set
    let any_kafka_set = std::env::vars().any(|(k, _)| k.starts_with("P_KAFKA_"));

    if !any_kafka_set {
        return vec![];
    }

    let mut prompts = vec![
        EnvPrompt {
            env_var: "P_KAFKA_BOOTSTRAP_SERVERS",
            display_name: "Kafka Bootstrap Servers",
            required: true,
            is_secret: false,
        },
        EnvPrompt {
            env_var: "P_KAFKA_CONSUMER_TOPICS",
            display_name: "Kafka Consumer Topics (comma-separated)",
            required: true,
            is_secret: false,
        },
    ];

    // Check security protocol for additional requirements
    const KAFKA_SECURITY_PROTOCOL_ENV: &str = "P_KAFKA_SECURITY_PROTOCOL";
    let protocol = std::env::var(KAFKA_SECURITY_PROTOCOL_ENV)
        .unwrap_or_default()
        .to_uppercase();

    let needs_ssl = matches!(protocol.as_str(), "SSL" | "SASL_SSL" | "SASL-SSL");
    let needs_sasl = matches!(
        protocol.as_str(),
        "SASL_PLAINTEXT" | "SASL-PLAINTEXT" | "SASL_SSL" | "SASL-SSL"
    );

    if needs_ssl {
        prompts.extend([
            EnvPrompt {
                env_var: "P_KAFKA_SSL_CA_LOCATION",
                display_name: "Kafka SSL CA Certificate Path",
                required: true,
                is_secret: false,
            },
            EnvPrompt {
                env_var: "P_KAFKA_SSL_CERTIFICATE_LOCATION",
                display_name: "Kafka SSL Client Certificate Path",
                required: true,
                is_secret: false,
            },
            EnvPrompt {
                env_var: "P_KAFKA_SSL_KEY_LOCATION",
                display_name: "Kafka SSL Client Key Path",
                required: true,
                is_secret: false,
            },
        ]);
    }

    if needs_sasl {
        prompts.extend([
            EnvPrompt {
                env_var: "P_KAFKA_SASL_MECHANISM",
                display_name: "Kafka SASL Mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI)",
                required: true,
                is_secret: false,
            },
            EnvPrompt {
                env_var: "P_KAFKA_SASL_USERNAME",
                display_name: "Kafka SASL Username",
                required: true,
                is_secret: false,
            },
            EnvPrompt {
                env_var: "P_KAFKA_SASL_PASSWORD",
                display_name: "Kafka SASL Password",
                required: true,
                is_secret: true,
            },
        ]);
    }

    prompts
}

/// Checks for missing enterprise-specific environment variables and
/// prompts for them interactively if running in a terminal.
///
/// Must be called **before** `PARSEABLE` is accessed and before license
/// verification, so that `.parseable.env` values are loaded and the user
/// gets a chance to provide missing vars like `P_CLUSTER_SECRET` and
/// the license file paths.
///
/// Returns collected `(env_var, value)` pairs. Call [`save_collected_envs`]
/// after validation succeeds to persist them.
pub fn prompt_enterprise_envs() -> Vec<(String, String)> {
    if is_help_or_version_request() {
        return vec![];
    }

    if detect_storage_subcommand().is_none() {
        return vec![];
    }

    // Load previously saved env vars so we pick up P_CLUSTER_SECRET etc.
    load_env_file();

    let is_interactive = io::stdin().is_terminal();
    let mut collected: Vec<(String, String)> = Vec::new();

    // Phase 1: base enterprise vars (license, cluster secret, mode)
    let base_prompts = get_enterprise_base_prompts();
    collect_prompts(
        &base_prompts,
        is_interactive,
        "Parseable Enterprise",
        &mut collected,
    );

    // Phase 2: now P_MODE is in the environment (from env, CLI, .parseable.env,
    // or the interactive prompt above), so mode-specific prompts resolve correctly.
    let mode_prompts = get_enterprise_mode_prompts();
    if !mode_prompts.is_empty() {
        let mode = detect_mode().unwrap_or_default();
        collect_prompts(
            &mode_prompts,
            is_interactive,
            &format!("{mode} mode"),
            &mut collected,
        );
    }

    collected
}

/// Collects missing required env vars from the given prompt list.
/// Prints a header and prompts interactively when running in a terminal.
fn collect_prompts(
    prompts: &[EnvPrompt],
    is_interactive: bool,
    context: &str,
    collected: &mut Vec<(String, String)>,
) {
    let missing: Vec<&EnvPrompt> = prompts
        .iter()
        .filter(|p| p.required && std::env::var(p.env_var).is_err())
        .collect();

    if missing.is_empty() {
        return;
    }

    if !is_interactive {
        return;
    }

    println!();
    println!("  Missing required environment variable(s) for {context}:");
    for m in &missing {
        println!("    - {} ({})", m.env_var, m.display_name);
    }
    println!();
    println!("  Starting interactive setup...");
    println!();

    for prompt in prompts {
        if std::env::var(prompt.env_var).is_ok() {
            continue;
        }

        let tag = if prompt.required {
            "required"
        } else {
            "optional, press Enter to skip"
        };

        let value = if prompt.is_secret {
            prompt_secret(&format!(
                "  {} ({}) [{}]: ",
                prompt.display_name, prompt.env_var, tag
            ))
        } else {
            prompt_line(&format!(
                "  {} ({}) [{}]: ",
                prompt.display_name, prompt.env_var, tag
            ))
        };

        let value = value.trim().to_string();

        if value.is_empty() {
            if prompt.required {
                eprintln!(
                    "  Error: {} is required and cannot be empty. Exiting.",
                    prompt.env_var
                );
                std::process::exit(1);
            }
            continue;
        }

        // SAFETY: Single-threaded startup, no other threads running.
        unsafe { std::env::set_var(prompt.env_var, &value) };
        collected.push((prompt.env_var.to_string(), value));
    }

    println!();
}

/// Returns base enterprise env var prompts (license, cluster secret, mode).
fn get_enterprise_base_prompts() -> Vec<EnvPrompt> {
    let mut prompts = vec![
        EnvPrompt {
            env_var: "P_LICENSE_DATA_FILE_PATH",
            display_name: "License Data File Path",
            required: true,
            is_secret: false,
        },
        EnvPrompt {
            env_var: "P_LICENSE_SIGNATURE_FILE_PATH",
            display_name: "License Signature File Path",
            required: true,
            is_secret: false,
        },
        EnvPrompt {
            env_var: "P_CLUSTER_SECRET",
            display_name: "Cluster Secret",
            required: true,
            is_secret: true,
        },
    ];

    // Enterprise rejects "all" mode, so prompt if not explicitly set
    if detect_mode().is_none() {
        prompts.push(EnvPrompt {
            env_var: "P_MODE",
            display_name: "Server Mode (query, ingest, index, prism)",
            required: true,
            is_secret: false,
        });
    }

    prompts
}

/// Returns mode-specific enterprise env var prompts.
/// Called after phase 1, so P_MODE is guaranteed to be in the environment.
fn get_enterprise_mode_prompts() -> Vec<EnvPrompt> {
    match detect_mode().as_deref() {
        Some("query") => vec![EnvPrompt {
            env_var: "P_HOT_TIER_DIR",
            display_name: "Hot Tier Directory Path",
            required: true,
            is_secret: false,
        }],
        Some("index") => vec![EnvPrompt {
            env_var: "P_INDEX_DIR",
            display_name: "Index Storage Directory Path",
            required: true,
            is_secret: false,
        }],
        _ => vec![],
    }
}

/// Prompts the user for a line of input (visible).
fn prompt_line(prompt: &str) -> String {
    print!("{prompt}");
    io::stdout().flush().expect("Failed to flush stdout");

    let mut input = String::default();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read input");
    input
}

/// Prompts the user for secret input (hidden using crossterm raw mode).
fn prompt_secret(prompt: &str) -> String {
    use crossterm::{
        event::{self, Event, KeyCode, KeyModifiers},
        terminal,
    };

    print!("{prompt}");
    io::stdout().flush().expect("Failed to flush stdout");

    terminal::enable_raw_mode().expect("Failed to enable raw mode");

    let mut input = String::default();
    loop {
        if let Ok(Event::Key(key_event)) = event::read() {
            match key_event.code {
                KeyCode::Enter => break,
                KeyCode::Backspace => {
                    input.pop();
                }
                KeyCode::Char('c') if key_event.modifiers.contains(KeyModifiers::CONTROL) => {
                    terminal::disable_raw_mode().expect("Failed to disable raw mode");
                    println!();
                    std::process::exit(130);
                }
                KeyCode::Char(c) => {
                    input.push(c);
                }
                _ => {}
            }
        }
    }

    terminal::disable_raw_mode().expect("Failed to disable raw mode");
    println!();

    input
}
