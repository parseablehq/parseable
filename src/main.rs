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

use parseable::{
    banner, kafka,
    option::{Mode, CONFIG},
    rbac, storage, IngestServer, ParseableServer, QueryServer, Server,
};
use tracing_subscriber::EnvFilter;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .compact()
        .init();

    match CONFIG.as_ref() {
        ParseableCommand::Completion { shell, output } => {
            let mut cmd = parseable::create_parseable_cli_command();
            let bin_name = cmd.get_name().to_owned();

            parseable::completions::generate_completion_script(
                &mut cmd,
                &bin_name,
                *shell,
                output.clone(),
            )?;
            return Ok(());
        }
        ParseableCommand::LocalStore(config)
        | ParseableCommand::S3Store(config)
        | ParseableCommand::BlobStore(config) => {
            // these are empty ptrs so mem footprint should be minimal
            let server: Box<dyn ParseableServer> = match config.parseable.mode {
                Mode::Query => Box::new(QueryServer),
                Mode::Ingest => Box::new(IngestServer),
                Mode::All => Box::new(Server),
            };

            // Load metadata from persistence
            let parseable_json = server.load_metadata().await?;
            let metadata = storage::resolve_parseable_metadata(&parseable_json).await?;
            banner::print(&CONFIG, &metadata).await;

            // Initialize the RBAC map
            rbac::map::init(&metadata);

            // Keep metadata info in memory
            metadata.set_global();

            // Load Kafka server if not in Query mode
            if config.parseable.mode != Mode::Query {
                tokio::task::spawn(kafka::setup_integration());
            }

            // Initialize the server
            server.init().await?;
        }
    }

    Ok(())
}
