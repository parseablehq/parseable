use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use once_cell::sync::Lazy;
use semver::Version;

pub static MODULE_REGISTRY: Lazy<Arc<RwLock<ModuleRegistry>>> = Lazy::new(Arc::default);

#[derive(Debug, Default)]
pub struct ModuleRegistry {
    inner: HashMap<String, Registration>,
}

impl ModuleRegistry {
    pub fn register(&mut self, module: Registration) {
        self.inner.insert(module.id.clone(), module);
    }

    pub fn registrations(&self) -> impl Iterator<Item = &Registration> {
        self.inner.values()
    }

    pub fn get(&self, id: &str) -> Option<&Registration> {
        self.inner.get(id)
    }
    pub fn deregister(&mut self, module_id: &str) {
        self.inner.remove(module_id);
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct StreamConfig {
    pub path: String,
    pub method: Method,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Registration {
    pub id: String,
    pub version: String,
    pub url: url::Url,
    pub username: String,
    pub password: String,
    pub stream_config: StreamConfig,
    pub routes: Vec<Route>,
}

impl Registration {
    pub fn contains_route(&self, path: &str, method: &http::Method) -> bool {
        self.routes
            .iter()
            .any(|x| x.module_path == path && method.eq(&x.method))
    }

    pub fn set_version(&mut self, version: &str) -> Result<(), String> {
        if let Some(version) = version.strip_prefix('v') {
            if Version::parse(version).is_err() {
                return Err("Invalid SemVer format".to_string());
            }
            self.version = version.to_string();
            Ok(())
        } else {
            Err("Module version must start with 'v'".to_string())
        }
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
