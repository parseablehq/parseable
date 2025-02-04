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
use serde_json::Value;

use crate::{
    migration::to_bytes, parseable::PARSEABLE, storage::object_storage::dashboard_path,
    utils::get_hash, LOCK_EXPECT,
};

use super::TimeFilter;

pub static DASHBOARDS: Lazy<Dashboards> = Lazy::new(Dashboards::default);
pub const CURRENT_DASHBOARD_VERSION: &str = "v3";

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Tiles {
    name: String,
    pub tile_id: Option<String>,
    description: String,
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
    tick_config: Vec<TickConfig>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct CircularChartConfig {
    name_key: String,
    value_key: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct GraphConfig {
    x_key: String,
    y_keys: Vec<String>,
    graph_type: Option<GraphType>,
    orientation: Option<Orientation>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
#[serde(rename_all = "lowercase")]
pub enum GraphType {
    #[default]
    Default,
    Stacked,
    Percent,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Orientation {
    #[default]
    Horizontal,
    Vertical,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct ColorConfig {
    field_name: String,
    color_palette: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct TickConfig {
    key: String,
    unit: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Dashboard {
    pub version: Option<String>,
    name: String,
    description: String,
    pub dashboard_id: Option<String>,
    pub user_id: Option<String>,
    pub time_filter: Option<TimeFilter>,
    refresh_interval: u64,
    pub tiles: Vec<Tiles>,
}

#[derive(Default, Debug)]
pub struct Dashboards(RwLock<Vec<Dashboard>>);

impl Dashboards {
    pub async fn load(&self) -> anyhow::Result<()> {
        let mut this = vec![];
        let store = PARSEABLE.storage.get_object_store();
        let all_dashboards = store.get_all_dashboards().await.unwrap_or_default();
        for (dashboard_relative_path, dashboards) in all_dashboards {
            for dashboard in dashboards {
                if dashboard.is_empty() {
                    continue;
                }
                let mut dashboard_value = serde_json::from_slice::<serde_json::Value>(&dashboard)?;
                if let Some(meta) = dashboard_value.clone().as_object() {
                    let version = meta.get("version").and_then(|version| version.as_str());
                    let dashboard_id = meta
                        .get("dashboard_id")
                        .and_then(|dashboard_id| dashboard_id.as_str());
                    match version {
                        Some("v1") => {
                            //delete older version of the dashboard
                            store.delete_object(&dashboard_relative_path).await?;

                            dashboard_value = migrate_v1_v2(dashboard_value);
                            dashboard_value = migrate_v2_v3(dashboard_value);
                            let user_id = dashboard_value
                                .as_object()
                                .unwrap()
                                .get("user_id")
                                .and_then(|user_id| user_id.as_str());
                            let path = dashboard_path(
                                user_id.unwrap(),
                                &format!("{}.json", dashboard_id.unwrap()),
                            );
                            let dashboard_bytes = to_bytes(&dashboard_value);
                            store.put_object(&path, dashboard_bytes.clone()).await?;
                        }
                        Some("v2") => {
                            //delete older version of the dashboard
                            store.delete_object(&dashboard_relative_path).await?;

                            dashboard_value = migrate_v2_v3(dashboard_value);
                            let user_id = dashboard_value
                                .as_object()
                                .unwrap()
                                .get("user_id")
                                .and_then(|user_id| user_id.as_str());
                            let path = dashboard_path(
                                user_id.unwrap(),
                                &format!("{}.json", dashboard_id.unwrap()),
                            );
                            let dashboard_bytes = to_bytes(&dashboard_value);
                            store.put_object(&path, dashboard_bytes.clone()).await?;
                        }
                        _ => {}
                    }
                }
                if let Ok(dashboard) = serde_json::from_value::<Dashboard>(dashboard_value) {
                    this.retain(|d: &Dashboard| d.dashboard_id != dashboard.dashboard_id);
                    this.push(dashboard);
                }
            }
        }

        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.append(&mut this);

        Ok(())
    }

    pub fn update(&self, dashboard: &Dashboard) {
        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.retain(|d| d.dashboard_id != dashboard.dashboard_id);
        s.push(dashboard.clone());
    }

    pub fn delete_dashboard(&self, dashboard_id: &str) {
        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.retain(|d| d.dashboard_id != Some(dashboard_id.to_string()));
    }

    pub fn get_dashboard(&self, dashboard_id: &str, user_id: &str) -> Option<Dashboard> {
        self.0
            .read()
            .expect(LOCK_EXPECT)
            .iter()
            .find(|d| {
                d.dashboard_id == Some(dashboard_id.to_string())
                    && d.user_id == Some(user_id.to_string())
            })
            .cloned()
    }

    pub fn list_dashboards_by_user(&self, user_id: &str) -> Vec<Dashboard> {
        self.0
            .read()
            .expect(LOCK_EXPECT)
            .iter()
            .filter(|d| d.user_id == Some(user_id.to_string()))
            .cloned()
            .collect()
    }
}

fn migrate_v1_v2(mut dashboard_meta: Value) -> Value {
    let dashboard_meta_map = dashboard_meta.as_object_mut().unwrap();
    let user_id = dashboard_meta_map.get("user_id").unwrap().clone();
    let str_user_id = user_id.as_str().unwrap();
    let user_id_hash = get_hash(str_user_id);
    dashboard_meta_map.insert("user_id".to_owned(), Value::String(user_id_hash));
    dashboard_meta_map.insert(
        "version".to_owned(),
        Value::String(CURRENT_DASHBOARD_VERSION.into()),
    );
    let tiles = dashboard_meta_map
        .get_mut("tiles")
        .unwrap()
        .as_array_mut()
        .unwrap();
    for tile in tiles.iter_mut() {
        let tile_map = tile.as_object_mut().unwrap();
        let visualization = tile_map
            .get_mut("visualization")
            .unwrap()
            .as_object_mut()
            .unwrap();
        visualization.insert("tick_config".to_owned(), Value::Array(vec![]));
    }

    dashboard_meta
}

fn migrate_v2_v3(mut dashboard_meta: Value) -> Value {
    let dashboard_meta_map = dashboard_meta.as_object_mut().unwrap();

    dashboard_meta_map.insert(
        "version".to_owned(),
        Value::String(CURRENT_DASHBOARD_VERSION.into()),
    );
    let tiles = dashboard_meta_map
        .get_mut("tiles")
        .unwrap()
        .as_array_mut()
        .unwrap();
    for tile in tiles {
        let tile_map = tile.as_object_mut().unwrap();
        let visualization = tile_map
            .get_mut("visualization")
            .unwrap()
            .as_object_mut()
            .unwrap();
        if visualization.get("graph_config").is_some()
            && !visualization.get("graph_config").unwrap().is_null()
        {
            let graph_config = visualization
                .get_mut("graph_config")
                .unwrap()
                .as_object_mut()
                .unwrap();
            graph_config.insert("orientation".to_owned(), Value::String("horizontal".into()));
            graph_config.insert("graph_type".to_owned(), Value::String("default".into()));
        }
    }

    dashboard_meta
}
