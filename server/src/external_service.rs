/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::storage::StorageMetadata;
use once_cell::sync::OnceCell;

pub static MODULE_REGISTRY: OnceCell<Arc<RwLock<ModuleRegistry>>> = OnceCell::new();

pub fn init(metadata: &StorageMetadata) {
    let mut registry = ModuleRegistry::default();
    registry.load_registry(metadata);

    MODULE_REGISTRY
        .set(Arc::new(RwLock::new(registry)))
        .expect("Module Registry is only set once");
}

pub fn global_module_registry() -> Arc<RwLock<ModuleRegistry>> {
    MODULE_REGISTRY
        .get()
        .expect("Module Registry initialized in main")
        .clone()
}

#[derive(Debug, Default)]
pub struct ModuleRegistry {
    inner: HashMap<String, Registration>,
}

impl ModuleRegistry {
    pub fn load_registry(&mut self, metadata: &StorageMetadata) {
        for (module_name, module) in &metadata.modules {
            self.inner.insert(module_name.clone(), module.clone());
        }
    }
    pub fn register(&mut self, module_name: String, module: Registration) {
        self.inner.insert(module_name, module);
    }

    pub fn get(&self, id: &str) -> Option<&Registration> {
        self.inner.get(id)
    }

    pub fn get_keys(&self) -> Vec<String> {
        self.inner.keys().cloned().collect()
    }

    pub fn deregister(&mut self, module_id: &str) {
        self.inner.remove(module_id);
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct StreamConfig {
    pub path: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Registration {
    pub version: String,
    pub url: url::Url,
    pub username: String,
    pub password: String,
    pub stream_config: StreamConfig,
    pub routes: Vec<Route>,
}

impl Registration {
    pub fn get_module_path(&self, path: &str, method: &http::Method) -> Option<String> {
        self.routes
            .iter()
            .find(|x| x.server_path == path && method.eq(&x.method))
            .map(|route| route.module_path.clone())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct DeRegistration {
    pub id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    pub server_path: String,
    pub module_path: String,
    pub method: Method,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "UPPERCASE")]
#[allow(clippy::upper_case_acronyms)]
pub enum Method {
    GET,
    PUT,
    POST,
    DELETE,
}

impl PartialEq<Method> for http::Method {
    fn eq(&self, other: &Method) -> bool {
        matches!(
            (self, other),
            (&http::Method::GET, &Method::GET)
                | (&http::Method::PUT, &Method::PUT)
                | (&http::Method::POST, &Method::POST)
                | (&http::Method::DELETE, &Method::DELETE)
        )
    }
}
