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
 *
 */

use crossterm::style::Stylize;
use human_size::SpecificSize;

use crate::about;
use crate::utils::uid::Uid;
use crate::{option::Config, storage::StorageMetadata};

pub async fn print(config: &Config, meta: &StorageMetadata) {
    print_ascii_art();
    let scheme = config.parseable.get_scheme();
    status_info(config, &scheme, meta.deployment_id);
    storage_info(config).await;
    about::print(config, meta).await;
    println!();
}

fn print_ascii_art() {
    let ascii_name = r#"
    `7MM"""Mq.                                             *MM        `7MM
      MM   `MM.                                             MM          MM
      MM   ,M9  ,6"Yb.  `7Mb,od8 ,pP"Ybd  .gP"Ya   ,6"Yb.   MM,dMMb.    MM   .gP"Ya
      MMmmdM9  8)   MM    MM' "' 8I   `" ,M'   Yb 8)   MM   MM    `Mb   MM  ,M'   Yb
      MM        ,pm9MM    MM     `YMMMa. 8M""""""  ,pm9MM   MM     M8   MM  8M""""""
      MM       8M   MM    MM     L.   I8 YM.    , 8M   MM   MM.   ,M9   MM  YM.    ,
    .JMML.     `Moo9^Yo..JMML.   M9mmmP'  `Mbmmd' `Moo9^Yo. P^YbmdP'  .JMML. `Mbmmd'
    "#;

    eprint!("{ascii_name}");
}

fn status_info(config: &Config, scheme: &str, id: Uid) {
    let address = format!(
        "\"{}://{}\" ({}), \":{}\" (gRPC)",
        scheme,
        config.parseable.address,
        scheme.to_ascii_uppercase(),
        config.parseable.grpc_port
    );

    let mut credentials =
        String::from("\"As set in P_USERNAME and P_PASSWORD environment variables\"");

    if config.is_default_creds() {
        credentials = "\"Using default creds admin, admin. Please set credentials with P_USERNAME and P_PASSWORD.\"".red().to_string();
    }

    let llm_status = match &config.parseable.open_ai_key {
        Some(_) => "OpenAI Configured".green(),
        None => "Not Configured".grey(),
    };

    eprintln!(
        "
    Welcome to Parseable Server! Deployment UID: \"{}\"",
        id.to_string(),
    );

    eprintln!(
        "
    {}
        Address:\t\t\t\t\t{}
        Credentials:\t\t\t\t\t{}
        Server Mode:\t\t\t\t\t\"{}\"
        LLM Status:\t\t\t\t\t\"{}\"",
        "Server:".to_string().bold(),
        address,
        credentials,
        llm_status,
        config.parseable.mode.to_str()
    );
}

/// Prints information about the `ObjectStorage`.
/// - Mode (`Local drive`, `S3 bucket`)
/// - Staging (temporary landing point for incoming events)
/// - Cache (local cache of data)
/// - Store (path where the data is stored and its latency)
async fn storage_info(config: &Config) {
    let storage = config.storage();
    let latency = storage.get_object_store().get_latency().await;

    eprintln!(
        "
    {}
        Storage Mode:\t\t\t\t\t\"{}\"
        Staging Path:\t\t\t\t\t\"{}\"",
        "Storage:".to_string().bold(),
        config.get_storage_mode_string(),
        config.staging_dir().to_string_lossy(),
    );

    if let Some(path) = &config.parseable.local_cache_path {
        let size: SpecificSize<human_size::Gigibyte> =
            SpecificSize::new(config.parseable.local_cache_size as f64, human_size::Byte)
                .unwrap()
                .into();

        eprintln!(
            "\
    {:8}Cache:\t\t\t\t\t\"{}\", (size: {})",
            "",
            path.display(),
            size
        );
    }

    eprintln!(
        "\
    {:8}Store:\t\t\t\t\t\t\"{}\", (latency: {:?})",
        "",
        storage.get_endpoint(),
        latency
    );
}
