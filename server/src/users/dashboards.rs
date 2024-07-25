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

use std::sync::RwLock;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::{metadata::LOCK_EXPECT, option::CONFIG};

use super::TimeFilter;

pub static DASHBOARDS: Lazy<Dashboards> = Lazy::new(Dashboards::default);
pub const CURRENT_DASHBOARD_VERSION: &str = "v1";

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Tiles {
    name: String,
    pub tile_id: Option<String>,
    description: String,
    stream: String,
    query: String,
    order: Option<u64>,
    visualization: Visualization,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Visualization {
    visualization_type: String,
    circular_chart_config: Option<CircularChartConfig>,
    graph_config: Option<GraphConfig>,
    size: String,
    color_config: Vec<ColorConfig>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct CircularChartConfig {
    name_key: String,
    value_key: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct GraphConfig {
    x_key: String,
    y_key: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct ColorConfig {
    field_name: String,
    color_palette: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Dashboard {
    pub version: Option<String>,
    name: String,
    description: String,
    pub dashboard_id: Option<String>,
    pub user_id: String,
    pub time_filter: Option<TimeFilter>,
    refresh_interval: u64,
    pub tiles: Vec<Tiles>,
}

#[derive(Default, Debug)]
pub struct Dashboards(RwLock<Vec<Dashboard>>);

impl Dashboards {
    pub async fn load(&self) -> anyhow::Result<()> {
        let mut this = vec![];
        let store = CONFIG.storage().get_object_store();
        let dashboards = store.get_all_dashboards().await.unwrap_or_default();

        for dashboard in dashboards {
            if let Ok(dashboard) = serde_json::from_slice::<Dashboard>(&dashboard) {
                this.push(dashboard);
            }
        }

        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.append(&mut this);

        Ok(())
    }

    pub fn update(&self, dashboard: &Dashboard) {
        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.retain(|f| f.dashboard_id != dashboard.dashboard_id);
        s.push(dashboard.clone());
    }

    pub fn delete_dashboard(&self, dashboard_id: &str) {
        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.retain(|f| f.dashboard_id != Some(dashboard_id.to_string()));
    }

    pub fn get_dashboard(&self, dashboard_id: &str) -> Option<Dashboard> {
        self.0
            .read()
            .expect(LOCK_EXPECT)
            .iter()
            .find(|f| f.dashboard_id == Some(dashboard_id.to_string()))
            .cloned()
    }

    pub fn list_dashboards_by_user(&self, user_id: &str) -> Vec<Dashboard> {
        self.0
            .read()
            .expect(LOCK_EXPECT)
            .iter()
            .filter(|f| f.user_id == user_id)
            .cloned()
            .collect()
    }
}
