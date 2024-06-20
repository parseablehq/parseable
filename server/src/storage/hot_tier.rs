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
use once_cell::sync::Lazy;
use std::path::PathBuf;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use crate::hottier::LocalHotTierManager;
use crate::metadata::STREAM_INFO;
use crate::option::CONFIG;
type SchedulerHandle = thread::JoinHandle<()>;

static SCHEDULER_HANDLER: Lazy<Mutex<Option<SchedulerHandle>>> = Lazy::new(|| Mutex::new(None));

fn async_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .thread_name("hot-tier-cleanup-task-thread")
        .enable_all()
        .build()
        .unwrap()
}

pub fn cleanup_hot_tier() {
    if CONFIG.is_hot_tier_enabled() {
        init_scheduler();
    }
}

fn init_scheduler() {
    log::info!("Setting up schedular");
    let mut scheduler = AsyncScheduler::new();
    let func = move || async {
        cleanup().await;
    };

    // Execute once on startup
    thread::spawn(move || {
        let rt = async_runtime();
        rt.block_on(func());
    });

    scheduler.every(1.day()).at("00:00").run(func);

    let scheduler_handler = thread::spawn(|| {
        let rt = async_runtime();
        rt.block_on(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                scheduler.run_pending().await;
            }
        });
    });

    *SCHEDULER_HANDLER.lock().unwrap() = Some(scheduler_handler);
    log::info!("Scheduler is initialized")
}

pub async fn cleanup() {
    let path = CONFIG.parseable.hot_tier_storage_path.as_ref().unwrap();
    let hot_tier_manager = LocalHotTierManager::global().unwrap();
    for stream in STREAM_INFO.list_streams() {
        let path = PathBuf::from(path).join(stream.clone());
        let hot_tier_enable = STREAM_INFO.hot_tier_enabled(&stream).unwrap();
        if !hot_tier_enable || !path.exists(){
            continue;
        }
        let files = std::fs::read_dir(path).unwrap().collect::<Vec<_>>();
        for file in files.iter(){
            match file{
                Err(err) => {
                    log::error!("Failed to read file: {:?}", err);
                    continue;
                }
                Ok(file)=>{
                    if file.path().extension().expect("should have an extension") == "parquet" {
                        let file_name = file
                            .file_name()
                            .to_str()
                            .expect("should be valid str")
                            .to_owned();
        
                        let date = file_name
                            .split_once('.')
                            .expect("should be valid split")
                            .0
                            .split_once('=')
                            .expect("should be valid split")
                            .1;
                        let date_time = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
                            .expect("should be valid date");
                        let time_delta = Utc::now().date_naive() - date_time;
                        if time_delta.num_days() > CONFIG.parseable.hot_tier_time_range {
                            if let Err(err) = hot_tier_manager
                                .delete_from_hot_tier(&stream, file.path())
                                .await
                            {
                                log::error!("Failed to delete file: {:?}", err);
                            }
                        }
                    }
                }
            }
            
        }
            
            }
        
    }

