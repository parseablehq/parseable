use std::sync::RwLock;

use once_cell::sync::Lazy;
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};

use crate::{handlers::http::users::USERS_ROOT_DIR, metadata::LOCK_EXPECT, option::CONFIG};

use super::TimeFilter;

pub static DASHBOARDS: Lazy<Dashboards> = Lazy::new(Dashboards::default);

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Pannel {
    stream_name: String,
    query: String,
    chart_type: String,
    columns: Vec<String>,
    headers: Vec<String>,
    dimensions: (u64, u64),
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Dashboard {
    version: String,
    name: String,
    id: String,
    time_filter: TimeFilter,
    refresh_interval: u64,
    pannels: Vec<Pannel>,
}

impl Dashboard {
    pub fn dashboard_id(&self) -> &str {
        &self.id
    }
}

#[derive(Default)]
pub struct Dashboards(RwLock<Vec<Dashboard>>);

impl Dashboards {
    pub async fn load(&self) -> anyhow::Result<()> {
        let mut this = vec![];
        let path = RelativePathBuf::from(USERS_ROOT_DIR);
        let store = CONFIG.storage().get_object_store();
        let objs = store
            .get_objects(Some(&path), Box::new(|path| path.ends_with(".json")))
            .await?;

        for obj in objs {
            if let Ok(filter) = serde_json::from_slice::<Dashboard>(&obj) {
                this.push(filter);
            }
        }

        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.append(&mut this);

        Ok(())
    }

    pub fn update(&self, dashboard: Dashboard) {
        let mut s = self.0.write().expect(LOCK_EXPECT);

        s.push(dashboard);
    }

    pub fn find(&self, dashboard_id: &str) -> Option<Dashboard> {
        self.0
            .read()
            .expect(LOCK_EXPECT)
            .iter()
            .find(|dashboard| dashboard.dashboard_id() == dashboard_id)
            .cloned()
    }
}
