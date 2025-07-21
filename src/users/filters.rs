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

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;

use super::TimeFilter;
use crate::{
    migration::to_bytes,
    parseable::PARSEABLE,
    rbac::{Users, map::SessionKey},
    storage::object_storage::filter_path,
    utils::{get_hash, user_auth_for_datasets, user_auth_for_query},
};

pub static FILTERS: Lazy<Filters> = Lazy::new(Filters::default);
pub const CURRENT_FILTER_VERSION: &str = "v2";
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Filter {
    pub version: Option<String>,
    pub user_id: Option<String>,
    pub stream_name: String,
    pub filter_name: String,
    pub filter_id: Option<String>,
    pub query: FilterQuery,
    pub time_filter: Option<TimeFilter>,
    /// all other fields are variable and can be added as needed
    #[serde(flatten)]
    pub other_fields: Option<serde_json::Map<String, Value>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FilterQuery {
    pub filter_type: FilterType,
    pub filter_query: Option<String>,
    pub filter_builder: Option<FilterBuilder>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FilterType {
    Filter,
    SQL,
    Search,
}

impl FilterType {
    pub fn to_str(&self) -> &str {
        match self {
            FilterType::Filter => "filter",
            FilterType::SQL => "sql",
            FilterType::Search => "search",
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FilterBuilder {
    pub id: String,
    pub combinator: String,
    pub rules: Vec<FilterRules>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FilterRules {
    pub id: String,
    pub combinator: String,
    pub rules: Vec<Rules>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Rules {
    pub id: String,
    pub field: String,
    pub value: String,
    pub operator: String,
}

#[derive(Debug, Default)]
pub struct Filters(RwLock<Vec<Filter>>);

impl Filters {
    pub async fn load(&self) -> anyhow::Result<()> {
        let mut this = vec![];
        let store = PARSEABLE.storage.get_object_store();
        let all_filters = store.get_all_saved_filters().await.unwrap_or_default();
        for (filter_relative_path, filters) in all_filters {
            for filter in filters {
                if filter.is_empty() {
                    continue;
                }
                let mut filter_value = serde_json::from_slice::<serde_json::Value>(&filter)?;
                if let Some(meta) = filter_value.clone().as_object() {
                    let version = meta.get("version").and_then(|version| version.as_str());

                    if version == Some("v1") {
                        //delete older version of the filter
                        store.delete_object(&filter_relative_path).await?;

                        filter_value = migrate_v1_v2(filter_value);
                        let user_id = filter_value
                            .as_object()
                            .unwrap()
                            .get("user_id")
                            .and_then(|user_id| user_id.as_str());
                        let filter_id = filter_value
                            .as_object()
                            .unwrap()
                            .get("filter_id")
                            .and_then(|filter_id| filter_id.as_str());
                        let stream_name = filter_value
                            .as_object()
                            .unwrap()
                            .get("stream_name")
                            .and_then(|stream_name| stream_name.as_str());
                        if let (Some(user_id), Some(stream_name), Some(filter_id)) =
                            (user_id, stream_name, filter_id)
                        {
                            let path =
                                filter_path(user_id, stream_name, &format!("{filter_id}.json"));
                            let filter_bytes = to_bytes(&filter_value);
                            store.put_object(&path, filter_bytes.clone()).await?;
                        }
                    }

                    if let Ok(filter) = serde_json::from_value::<Filter>(filter_value) {
                        this.retain(|f: &Filter| f.filter_id != filter.filter_id);
                        this.push(filter);
                    }
                }
            }
        }

        let mut s = self.0.write().await;
        s.append(&mut this);

        Ok(())
    }

    pub async fn update(&self, filter: &Filter) {
        let mut s = self.0.write().await;
        s.retain(|f| f.filter_id != filter.filter_id);
        s.push(filter.clone());
    }

    pub async fn delete_filter(&self, filter_id: &str) {
        let mut s = self.0.write().await;
        s.retain(|f| f.filter_id != Some(filter_id.to_string()));
    }

    pub async fn get_filter(&self, filter_id: &str, user_id: &str) -> Option<Filter> {
        self.0
            .read()
            .await
            .iter()
            .find(|f| {
                f.filter_id == Some(filter_id.to_string()) && f.user_id == Some(user_id.to_string())
            })
            .cloned()
    }

    pub async fn list_filters(&self, key: &SessionKey) -> Vec<Filter> {
        let read = self.0.read().await;

        let mut filters = Vec::new();
        let permissions = Users.get_permissions(key);
        for f in read.iter() {
            let query: &str = f.query.filter_query.as_deref().unwrap_or("");
            let filter_type = &f.query.filter_type;

            // if filter type is SQL, check if the user has access to the dataset based on the query string
            // if filter type is search or filter, check if the user has access to the dataset based on the dataset name
            if *filter_type == FilterType::SQL {
                if (user_auth_for_query(key, query).await).is_ok() {
                    filters.push(f.clone())
                }
            } else if *filter_type == FilterType::Search || *filter_type == FilterType::Filter {
                let dataset_name = &f.stream_name;
                if user_auth_for_datasets(&permissions, &[dataset_name.to_string()])
                    .await
                    .is_ok()
                {
                    filters.push(f.clone())
                }
            }
        }
        filters
    }
}

fn migrate_v1_v2(mut filter_meta: Value) -> Value {
    let filter_meta_map = filter_meta.as_object_mut().unwrap();
    let user_id = filter_meta_map.get("user_id").unwrap().clone();
    let str_user_id = user_id.as_str().unwrap();
    let user_id_hash = get_hash(str_user_id);
    filter_meta_map.insert("user_id".to_owned(), Value::String(user_id_hash));
    filter_meta_map.insert(
        "version".to_owned(),
        Value::String(CURRENT_FILTER_VERSION.into()),
    );

    filter_meta
}
