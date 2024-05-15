use std::sync::RwLock;

use once_cell::sync::Lazy;
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};

use super::TimeFilter;
use crate::{handlers::http::users::USERS_ROOT_DIR, metadata::LOCK_EXPECT, option::CONFIG};

pub static FILTERS: Lazy<Filters> = Lazy::new(Filters::default);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Filter {
    version: String,
    stream_name: String,
    filter_name: String,
    filter_id: String,
    query: String,
    time_filter: Option<TimeFilter>,
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
            .await?;

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
