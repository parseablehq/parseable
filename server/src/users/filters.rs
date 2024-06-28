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
use std::sync::RwLock;

use super::TimeFilter;
use crate::{metadata::LOCK_EXPECT, option::CONFIG};

pub static FILTERS: Lazy<Filters> = Lazy::new(Filters::default);
pub const CURRENT_FILTER_VERSION: &str = "v1";
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Filter {
    pub version: Option<String>,
    pub user_id: String,
    pub stream_name: String,
    pub filter_name: String,
    pub filter_id: Option<String>,
    pub query: FilterQuery,
    pub time_filter: Option<TimeFilter>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FilterQuery {
    pub filter_type: String,
    pub filter_query: Option<String>,
    pub filter_builder: Option<FilterBuilder>,
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
        let store = CONFIG.storage().get_object_store();
        let filters = store.get_all_saved_filters().await.unwrap_or_default();

        for filter in filters {
            if let Ok(filter) = serde_json::from_slice::<Filter>(&filter) {
                this.push(filter);
            }
        }

        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.append(&mut this);

        Ok(())
    }

    pub fn update(&self, filter: &Filter) {
        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.retain(|f| f.filter_id != filter.filter_id);
        s.push(filter.clone());
    }

    pub fn delete_filter(&self, filter_id: &str) {
        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.retain(|f| f.filter_id != Some(filter_id.to_string()));
    }

    pub fn get_filter(&self, filter_id: &str) -> Option<Filter> {
        self.0
            .read()
            .expect(LOCK_EXPECT)
            .iter()
            .find(|f| f.filter_id == Some(filter_id.to_string()))
            .cloned()
    }

    pub fn list_filters_by_user(&self, user_id: &str) -> Vec<Filter> {
        self.0
            .read()
            .expect(LOCK_EXPECT)
            .iter()
            .filter(|f| f.user_id == user_id)
            .cloned()
            .collect()
    }
}
