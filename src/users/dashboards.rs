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

use bytes::Bytes;
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;
use ulid::Ulid;

use crate::{
    handlers::http::users::dashboards::DashboardError, parseable::PARSEABLE,
    storage::object_storage::dashboard_path,
};

pub static DASHBOARDS: Lazy<Dashboards> = Lazy::new(Dashboards::default);
pub const CURRENT_DASHBOARD_VERSION: &str = "v1";

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
pub enum DashboardType {
    #[default]
    Dashboard,
    Report,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Tile {
    pub tile_id: Ulid,
    #[serde(flatten)]
    pub other_fields: Option<serde_json::Map<String, Value>>,
}
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Dashboard {
    pub version: Option<String>,
    pub title: String,
    pub author: Option<String>,
    pub dashboard_id: Option<Ulid>,
    pub modified: Option<DateTime<Utc>>,
    dashboard_type: Option<DashboardType>,
    pub tiles: Vec<Tile>,
}

#[derive(Default, Debug)]
pub struct Dashboards(RwLock<Vec<Dashboard>>);

impl Dashboards {
    pub async fn load(&self) -> anyhow::Result<()> {
        let mut this = vec![];
        let store = PARSEABLE.storage.get_object_store();
        let all_dashboards = store.get_all_dashboards().await.unwrap_or_default();
        for (_, dashboards) in all_dashboards {
            for dashboard in dashboards {
                if dashboard.is_empty() {
                    continue;
                }
                let dashboard_value = serde_json::from_slice::<serde_json::Value>(&dashboard)?;
                if let Ok(dashboard) = serde_json::from_value::<Dashboard>(dashboard_value) {
                    this.retain(|d: &Dashboard| d.dashboard_id != dashboard.dashboard_id);
                    this.push(dashboard);
                }
            }
        }

        let mut s = self.0.write().await;
        s.append(&mut this);

        Ok(())
    }

    pub async fn create(
        &self,
        user_id: &str,
        dashboard: &mut Dashboard,
    ) -> Result<(), DashboardError> {
        let mut s = self.0.write().await;
        let dashboard_id = Ulid::new();
        dashboard.author = Some(user_id.to_string());
        dashboard.dashboard_id = Some(dashboard_id);
        dashboard.version = Some(CURRENT_DASHBOARD_VERSION.to_string());
        dashboard.modified = Some(Utc::now());
        dashboard.dashboard_type = Some(DashboardType::Dashboard);
        for tile in dashboard.tiles.iter_mut() {
            tile.tile_id = Ulid::new();
        }
        s.push(dashboard.clone());

        let path = dashboard_path(user_id, &format!("{}.json", dashboard_id));

        let store = PARSEABLE.storage.get_object_store();
        let dashboard_bytes = serde_json::to_vec(&dashboard)?;
        store
            .put_object(&path, Bytes::from(dashboard_bytes))
            .await?;
        Ok(())
    }

    pub async fn update(
        &self,
        user_id: &str,
        dashboard_id: Ulid,
        dashboard: &mut Dashboard,
    ) -> Result<(), DashboardError> {
        let mut s = self.0.write().await;
        if self.get_dashboard(dashboard_id).await.is_none() {
            return Err(DashboardError::Metadata("Dashboard does not exist"));
        }
        dashboard.dashboard_id = Some(dashboard_id);
        dashboard.modified = Some(Utc::now());
        dashboard.version = Some(CURRENT_DASHBOARD_VERSION.to_string());
        for tile in dashboard.tiles.iter_mut() {
            if tile.tile_id.is_nil() {
                tile.tile_id = Ulid::new();
            }
        }
        s.retain(|d| d.dashboard_id != dashboard.dashboard_id);
        s.push(dashboard.clone());

        let path = dashboard_path(user_id, &format!("{}.json", dashboard_id));

        let store = PARSEABLE.storage.get_object_store();
        let dashboard_bytes = serde_json::to_vec(&dashboard)?;
        store
            .put_object(&path, Bytes::from(dashboard_bytes))
            .await?;
        Ok(())
    }

    pub async fn delete_dashboard(
        &self,
        user_id: &str,
        dashboard_id: Ulid,
    ) -> Result<(), DashboardError> {
        let mut s = self.0.write().await;

        if self.get_dashboard(dashboard_id).await.is_none() {
            return Err(DashboardError::Metadata("Dashboard does not exist"));
        }
        s.retain(|d| *d.dashboard_id.as_ref().unwrap() != dashboard_id);
        let path = dashboard_path(user_id, &format!("{}.json", dashboard_id));
        let store = PARSEABLE.storage.get_object_store();
        store.delete_object(&path).await?;

        Ok(())
    }

    pub async fn get_dashboard(&self, dashboard_id: Ulid) -> Option<Dashboard> {
        self.0
            .read()
            .await
            .iter()
            .find(|d| *d.dashboard_id.as_ref().unwrap() == dashboard_id)
            .cloned()
    }

    pub async fn list_dashboards(&self) -> Vec<Dashboard> {
        let read = self.0.read().await;

        let mut dashboards = Vec::new();

        for d in read.iter() {
            dashboards.push(d.clone());
        }
        dashboards
    }
}
