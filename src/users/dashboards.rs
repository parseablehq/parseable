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

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;
use ulid::Ulid;

use crate::{
    handlers::http::users::{DASHBOARDS_DIR, USERS_ROOT_DIR, dashboards::DashboardError},
    metastore::metastore_traits::MetastoreObject,
    parseable::PARSEABLE,
};

pub static DASHBOARDS: Lazy<Dashboards> = Lazy::new(Dashboards::default);
pub const CURRENT_DASHBOARD_VERSION: &str = "v1";

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
/// type of dashboard
/// Dashboard is the default type
/// Report is a type of dashboard that is used for reporting
pub enum DashboardType {
    /// Dashboard is the default type
    #[default]
    Dashboard,
    /// Report is a type of dashboard that is used for reporting
    Report,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Tile {
    pub tile_id: Ulid,
    /// all other fields are variable and can be added as needed
    #[serde(flatten)]
    pub other_fields: Option<serde_json::Map<String, Value>>,
}
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Dashboard {
    pub version: Option<String>,
    pub title: String,
    pub author: Option<String>,
    pub dashboard_id: Option<Ulid>,
    pub created: Option<DateTime<Utc>>,
    pub modified: Option<DateTime<Utc>>,
    pub tags: Option<Vec<String>>,
    pub is_favorite: Option<bool>, // whether the dashboard is marked as favorite, default is false
    dashboard_type: Option<DashboardType>,
    pub tiles: Option<Vec<Tile>>,
}

impl MetastoreObject for Dashboard {
    fn get_object_path(&self) -> String {
        RelativePathBuf::from_iter([
            USERS_ROOT_DIR,
            self.author.as_ref().unwrap(),
            DASHBOARDS_DIR,
            &format!("{}.json", self.dashboard_id.unwrap()),
        ])
        .to_string()
    }

    fn get_object_id(&self) -> String {
        self.dashboard_id.unwrap().to_string()
    }
}

impl Dashboard {
    /// set metadata for the dashboard
    /// add author, dashboard_id, version, modified, and dashboard_type
    /// if dashboard_id is None, generate a new one
    pub fn set_metadata(&mut self, user_id: &str, dashboard_id: Option<Ulid>) {
        self.author = Some(user_id.to_string());
        self.dashboard_id = dashboard_id.or_else(|| Some(Ulid::new()));
        self.version = Some(CURRENT_DASHBOARD_VERSION.to_string());
        self.modified = Some(Utc::now());
        if self.dashboard_type.is_none() {
            self.dashboard_type = Some(DashboardType::Dashboard);
        }
        if self.tiles.is_none() {
            self.tiles = Some(Vec::new());
        }

        // if is_favorite is None, set it to false, else set it to the current value
        self.is_favorite = self.is_favorite.or(Some(false));
    }

    /// create a summary of the dashboard
    /// used for listing dashboards
    pub fn to_summary(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut map = serde_json::Map::new();

        map.insert(
            "title".to_string(),
            serde_json::Value::String(self.title.clone()),
        );

        if let Some(author) = &self.author {
            map.insert(
                "author".to_string(),
                serde_json::Value::String(author.to_string()),
            );
        }

        if let Some(created) = &self.created {
            map.insert(
                "created".to_string(),
                serde_json::Value::String(created.to_string()),
            );
        }

        if let Some(modified) = &self.modified {
            map.insert(
                "modified".to_string(),
                serde_json::Value::String(modified.to_string()),
            );
        }

        if let Some(dashboard_id) = &self.dashboard_id {
            map.insert(
                "dashboardId".to_string(),
                serde_json::Value::String(dashboard_id.to_string()),
            );
        }

        if let Some(tags) = &self.tags {
            map.insert(
                "tags".to_string(),
                serde_json::Value::Array(
                    tags.iter()
                        .map(|tag| serde_json::Value::String(tag.clone()))
                        .collect(),
                ),
            );
        }

        map.insert(
            "isFavorite".to_string(),
            serde_json::Value::Bool(self.is_favorite.unwrap_or(false)),
        );

        map
    }
}

/// Validate the dashboard ID
/// Check if the dashboard ID is a valid ULID
/// If the dashboard ID is not valid, return an error
pub fn validate_dashboard_id(dashboard_id: String) -> Result<Ulid, DashboardError> {
    Ulid::from_string(&dashboard_id)
        .map_err(|_| DashboardError::Metadata("Invalid dashboard ID format - must be a valid ULID"))
}

#[derive(Default, Debug)]
pub struct Dashboards(RwLock<Vec<Dashboard>>);

impl Dashboards {
    /// Load all dashboards from the object store
    /// and store them in memory
    /// This function is called on server start
    pub async fn load(&self) -> anyhow::Result<()> {
        let mut this = vec![];

        let all_dashboards = PARSEABLE.metastore.get_dashboards().await?;

        for dashboard in all_dashboards {
            if dashboard.is_empty() {
                continue;
            }

            let dashboard_value = match serde_json::from_slice::<serde_json::Value>(&dashboard) {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!("Failed to parse dashboard JSON: {}", err);
                    continue;
                }
            };

            if let Ok(dashboard) = serde_json::from_value::<Dashboard>(dashboard_value.clone()) {
                this.retain(|d: &Dashboard| d.dashboard_id != dashboard.dashboard_id);
                this.push(dashboard);
            } else {
                tracing::warn!("Failed to deserialize dashboard: {:?}", dashboard_value);
            }
        }

        let mut s = self.0.write().await;
        s.append(&mut this);

        Ok(())
    }

    /// Save the dashboard to the object store
    /// This function is called when creating or updating a dashboard
    async fn save_dashboard(
        &self,
        // user_id: &str,
        dashboard: &Dashboard,
    ) -> Result<(), DashboardError> {
        PARSEABLE.metastore.put_dashboard(dashboard).await?;

        Ok(())
    }

    /// Create a new dashboard
    /// This function is called when creating a new dashboard
    /// add dashboard in memory and save it to the object store
    pub async fn create(
        &self,
        user_id: &str,
        dashboard: &mut Dashboard,
    ) -> Result<(), DashboardError> {
        dashboard.created = Some(Utc::now());
        dashboard.set_metadata(user_id, None);

        let mut dashboards = self.0.write().await;

        let has_duplicate = dashboards
            .iter()
            .any(|d| d.title == dashboard.title && d.dashboard_id != dashboard.dashboard_id);

        if has_duplicate {
            return Err(DashboardError::Metadata("Dashboard title must be unique"));
        }

        self.save_dashboard(dashboard).await?;

        dashboards.push(dashboard.clone());

        Ok(())
    }

    /// Update an existing dashboard
    /// This function is called when updating a dashboard
    /// update dashboard in memory and save it to the object store
    pub async fn update(
        &self,
        user_id: &str,
        dashboard_id: Ulid,
        dashboard: &mut Dashboard,
    ) -> Result<(), DashboardError> {
        let mut dashboards = self.0.write().await;

        let existing_dashboard = dashboards
            .iter()
            .find(|d| d.dashboard_id == Some(dashboard_id) && d.author == Some(user_id.to_string()))
            .cloned()
            .ok_or_else(|| {
                DashboardError::Metadata(
                    "Dashboard does not exist or you do not have permission to access it",
                )
            })?;

        dashboard.set_metadata(user_id, Some(dashboard_id));
        dashboard.created = existing_dashboard.created;

        let has_duplicate = dashboards
            .iter()
            .any(|d| d.title == dashboard.title && d.dashboard_id != dashboard.dashboard_id);

        if has_duplicate {
            return Err(DashboardError::Metadata("Dashboard title must be unique"));
        }

        self.save_dashboard(dashboard).await?;

        dashboards.retain(|d| d.dashboard_id != Some(dashboard_id));
        dashboards.push(dashboard.clone());

        Ok(())
    }

    /// Delete a dashboard
    /// This function is called when deleting a dashboard
    /// delete dashboard in memory and from the object store
    pub async fn delete_dashboard(
        &self,
        user_id: &str,
        dashboard_id: Ulid,
    ) -> Result<(), DashboardError> {
        let obj = self.ensure_dashboard_ownership(dashboard_id, user_id)
            .await?;

        {
            // validation has happened, dashboard exists and can be deleted by the user
            PARSEABLE.metastore.delete_dashboard(&obj).await?;
        }

        // delete from in-memory
        self.0
            .write()
            .await
            .retain(|d| d.dashboard_id != Some(dashboard_id));

        Ok(())
    }

    /// Get a dashboard by ID
    /// fetch dashboard from memory
    pub async fn get_dashboard(&self, dashboard_id: Ulid) -> Option<Dashboard> {
        self.0
            .read()
            .await
            .iter()
            .find(|d| {
                d.dashboard_id
                    .as_ref()
                    .is_some_and(|id| *id == dashboard_id)
            })
            .cloned()
    }

    /// Get a dashboard by ID and user ID
    /// fetch dashboard from memory
    pub async fn get_dashboard_by_user(
        &self,
        dashboard_id: Ulid,
        user_id: &str,
    ) -> Option<Dashboard> {
        self.0
            .read()
            .await
            .iter()
            .find(|d| {
                d.dashboard_id
                    .as_ref()
                    .is_some_and(|id| *id == dashboard_id)
                    && d.author == Some(user_id.to_string())
            })
            .cloned()
    }

    /// List all dashboards
    /// fetch all dashboards from memory
    pub async fn list_dashboards(&self, limit: usize) -> Vec<Dashboard> {
        // limit the number of dashboards returned in order of modified date
        // if limit is 0, return all dashboards
        let dashboards = self.0.read().await;
        let mut sorted_dashboards = dashboards
            .iter()
            .filter(|d| d.dashboard_id.is_some())
            .cloned()
            .collect::<Vec<Dashboard>>();
        sorted_dashboards.sort_by_key(|d| std::cmp::Reverse(d.modified));
        if limit > 0 {
            sorted_dashboards.truncate(limit);
        }
        sorted_dashboards
    }

    /// List tags from all dashboards
    /// This function returns a list of unique tags from all dashboards
    pub async fn list_tags(&self) -> Vec<String> {
        let dashboards = self.0.read().await;
        let mut tags = dashboards
            .iter()
            .filter_map(|d| d.tags.as_ref())
            .flat_map(|t| t.iter().cloned())
            .collect::<Vec<String>>();
        tags.sort();
        tags.dedup();
        tags
    }

    /// List dashboards by tag
    /// This function returns a list of dashboards that match any of the provided tags
    /// If no tags are provided, it returns an empty list
    pub async fn list_dashboards_by_tags(&self, tags: Vec<String>) -> Vec<Dashboard> {
        let dashboards = self.0.read().await;
        dashboards
            .iter()
            .filter(|d| {
                if let Some(dashboard_tags) = &d.tags {
                    dashboard_tags
                        .iter()
                        .any(|dashboard_tag| tags.contains(dashboard_tag))
                } else {
                    false
                }
            })
            .cloned()
            .collect()
    }

    /// Ensure the user is the owner of the dashboard
    /// This function is called when updating or deleting a dashboard
    /// check if the user is the owner of the dashboard
    /// if the user is not the owner, return an error
    async fn ensure_dashboard_ownership(
        &self,
        dashboard_id: Ulid,
        user_id: &str,
    ) -> Result<Dashboard, DashboardError> {
        self.get_dashboard_by_user(dashboard_id, user_id)
            .await
            .ok_or_else(|| {
                DashboardError::Metadata(
                    "Dashboard does not exist or you do not have permission to access it",
                )
            })
    }
}
