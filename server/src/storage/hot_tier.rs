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

use chrono::Utc;
use clokwerk::{AsyncScheduler, Job, TimeUnits};
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs as AsyncFs;

use crate::metadata::STREAM_INFO;
use crate::option::CONFIG;

async fn cleanup() {
    let path = CONFIG.parseable.hot_tier_storage_path.as_ref().unwrap();
    let cleanup_interval = CONFIG
        .parseable
        .hot_tier_time_range
        .expect("alredy checked for none");
    let streams = STREAM_INFO.list_streams();

    let now = Utc::now().date_naive();

    for stream in streams {
        let path = PathBuf::from(path).join(stream);
        let mut files = AsyncFs::read_dir(path).await.unwrap();

        while let Ok(file) = files.next_entry().await {
            if let Some(file) = file {
                if file.path().extension().expect("should have an extension") == "parquet" {
                    let file_str = file
                        .file_name()
                        .to_str()
                        .expect("should be valid str")
                        .to_owned();
                    // 2024-05-24

                    let date = file_str
                        .split_once('.')
                        .expect("should be valid split")
                        .0
                        .split_once('=')
                        .expect("should be valid split")
                        .0;

                    let date_time = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
                        .expect("should be valid date");

                    let time_delta = now - date_time;
                    if time_delta.num_days() > cleanup_interval {
                        if let Err(err) = AsyncFs::remove_file(file.path()).await {
                            log::error!("Failed to remove file: {:?}", err);
                        }
                    }
                }
            }
        }
    }
}

async fn run() -> anyhow::Result<()> {
    log::info!("Setting up schedular for hot tier files cleanup");

    let mut scheduler = AsyncScheduler::new();
    scheduler.every(1u32.day()).at("00:00").run(cleanup);

    tokio::spawn(async move {
        loop {
            scheduler.run_pending().await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    Ok(())
}

pub async fn setup_hot_tier_scheduler() -> anyhow::Result<()> {
    if CONFIG.is_hot_tier_enabled() {
        run().await?;
    }

    Ok(())
}
