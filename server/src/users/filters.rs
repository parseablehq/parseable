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
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};
use std::sync::RwLock;

use super::TimeFilter;
use crate::{handlers::http::users::USERS_ROOT_DIR, metadata::LOCK_EXPECT, option::CONFIG};

pub static FILTERS: Lazy<Filters> = Lazy::new(Filters::default);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Filter {
    version: String,
    stream_name: String,
    filter_name: String,
    filter_id: String,
    query: FilterQuery,
    time_filter: Option<TimeFilter>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FilterQuery {
    filter_type: String,
    filter_query: Option<String>,
    filter_builder: Option<FilterBuilder>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FilterBuilder {
    id: String,
    combinator: String,
    rules: Vec<FilterRules>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FilterRules {
    id: String,
    combinator: String,
    rules: Vec<Rules>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Rules {
    id: String,
    field: String,
    value: String,
    operator: String,
}

impl Filter {
    pub fn filter_id(&self) -> &str {
        &self.filter_id
    }
}

#[derive(Debug, Default)]
pub struct Filters(RwLock<Vec<Filter>>);

impl Filters {
    pub async fn load(&self) -> anyhow::Result<()> {
        let mut this = vec![];
        let path = RelativePathBuf::from(USERS_ROOT_DIR);
        let store = CONFIG.storage().get_object_store();

        let objs = store
            .get_objects(Some(&path), Box::new(|path| path.ends_with(".json")))
            .await
            .unwrap_or_default();

        for obj in objs {
            if let Ok(filter) = serde_json::from_slice::<Filter>(&obj) {
                this.push(filter);
            }
        }

        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.append(&mut this);

        Ok(())
    }

    pub fn update(&self, filter: Filter) {
        let mut s = self.0.write().expect(LOCK_EXPECT);
        s.retain(|f| f.filter_id() != filter.filter_id());
        s.push(filter);
    }

    pub fn find(&self, filter_id: &str) -> Option<Filter> {
        self.0
            .read()
            .expect(LOCK_EXPECT)
            .iter()
            .find(|filter| filter.filter_id() == filter_id)
            .cloned()
    }
}
