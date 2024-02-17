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

use std::hash::Hash;
use std::num::NonZeroU32;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use clokwerk::AsyncScheduler;
use clokwerk::Job;
use clokwerk::TimeUnits;
use derive_more::Display;
use once_cell::sync::Lazy;

use crate::metadata::STREAM_INFO;
use crate::option::CONFIG;

type SchedulerHandle = thread::JoinHandle<()>;

static SCHEDULER_HANDLER: Lazy<Mutex<Option<SchedulerHandle>>> = Lazy::new(|| Mutex::new(None));

fn async_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .thread_name("retention-task-thread")
        .enable_all()
        .build()
        .unwrap()
}

pub fn load_retention_from_global() {
    log::info!("loading retention for all streams");
    init_scheduler();
}

pub fn init_scheduler() {
    log::info!("Setting up schedular");
    let mut scheduler = AsyncScheduler::new();
    let func = move || async {
        for stream in STREAM_INFO.list_streams() {
            let res = CONFIG
                .storage()
                .get_object_store()
                .get_retention(&stream)
                .await;

            match res {
                Ok(config) => {
                    for Task { action, days, .. } in config.tasks.into_iter() {
                        match action {
                            Action::Delete => {
                                let stream = stream.to_string();
                                thread::spawn(move || {
                                    let rt = tokio::runtime::Runtime::new().unwrap();
                                    rt.block_on(async {
                                        // Run the asynchronous delete action
                                        action::delete(stream.clone(), u32::from(days)).await;
                                    });
                                });
                            }
                        };
                    }
                }
                Err(err) => {
                    log::warn!("failed to load retention config for {stream} due to {err:?}")
                }
            };
        }
    };

    scheduler.every(1.day()).at("00:00").run(func);

    let handler = thread::spawn(|| {
        let rt = async_runtime();
        rt.block_on(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                scheduler.run_pending().await;
            }
        });
    });

    *SCHEDULER_HANDLER.lock().unwrap() = Some(handler);
    log::info!("Scheduler is initialized")
}

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "Vec<TaskView>")]
#[serde(into = "Vec<TaskView>")]
pub struct Retention {
    tasks: Vec<Task>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Task {
    description: String,
    action: Action,
    days: NonZeroU32,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Display, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
enum Action {
    Delete,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TaskView {
    description: String,
    action: Action,
    duration: String,
}

impl Retention {
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }
}

impl TryFrom<Vec<TaskView>> for Retention {
    type Error = String;

    fn try_from(task_view: Vec<TaskView>) -> Result<Self, Self::Error> {
        let mut set = Vec::with_capacity(2);
        let mut tasks = Vec::new();

        for task in task_view {
            let duration = task.duration;
            if !duration.ends_with('d') {
                return Err("missing 'd' suffix for duration value".to_string());
            }
            let Ok(days) = duration[0..duration.len() - 1].parse() else {
                return Err("could not convert duration to an unsigned number".to_string());
            };

            if set.contains(&task.action) {
                return Err(format!(
                    "Configuration contains two task both of action \"{}\"",
                    task.action
                ));
            } else {
                set.push(task.action)
            }

            tasks.push(Task {
                description: task.description,
                action: task.action,
                days,
            })
        }

        Ok(Retention { tasks })
    }
}

impl From<Retention> for Vec<TaskView> {
    fn from(value: Retention) -> Self {
        value
            .tasks
            .into_iter()
            .map(|task| {
                let duration = format!("{}d", task.days);
                TaskView {
                    description: task.description,
                    action: task.action,
                    duration,
                }
            })
            .collect()
    }
}

mod action {
    use chrono::{Days, NaiveDate, Utc};
    use futures::{stream::FuturesUnordered, StreamExt};
    use itertools::Itertools;
    use relative_path::RelativePathBuf;

    use crate::option::CONFIG;

    pub(super) async fn delete(stream_name: String, days: u32) {
        log::info!("running retention task - delete for stream={stream_name}");
        let retain_until = get_retain_until(Utc::now().date_naive(), days as u64);

        let Ok(dates) = CONFIG
            .storage()
            .get_object_store()
            .list_dates(&stream_name)
            .await
        else {
            return;
        };

        let dates_to_delete = dates
            .into_iter()
            .filter(|date| string_to_date(date) < retain_until)
            .collect_vec();

        let delete_tasks = FuturesUnordered::new();
        for date in dates_to_delete {
            let path = RelativePathBuf::from_iter([&stream_name, &date]);
            delete_tasks.push(async move {
                CONFIG
                    .storage()
                    .get_object_store()
                    .delete_prefix(&path)
                    .await
            });
        }

        let res: Vec<_> = delete_tasks.collect().await;

        for res in res {
            if let Err(err) = res {
                log::error!("Failed to run delete task {err:?}")
            }
        }
    }

    fn get_retain_until(current_date: NaiveDate, days: u64) -> NaiveDate {
        current_date - Days::new(days)
    }

    fn string_to_date(date: &str) -> NaiveDate {
        let year = date[5..9].parse().unwrap();
        let month = date[10..12].parse().unwrap();
        let day = date[13..15].parse().unwrap();

        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    #[cfg(test)]
    mod tests {
        use chrono::{Datelike, NaiveDate};

        use super::get_retain_until;
        use super::string_to_date;

        #[test]
        fn test_time_from_string() {
            let value = "date=2000-01-01";
            let time = string_to_date(value);
            assert_eq!(time, NaiveDate::from_ymd_opt(2000, 1, 1).unwrap());
        }
        #[test]
        fn test_retain_day() {
            let current_date = NaiveDate::from_ymd_opt(2000, 1, 2).unwrap();
            let date = get_retain_until(current_date, 1);
            assert_eq!(date.day(), 1)
        }
    }
}
