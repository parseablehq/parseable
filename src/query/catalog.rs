use std::any::Any;
use std::sync::{Arc, Weak};

use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};

use datafusion::common::plan_datafusion_err;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;

use async_trait::async_trait;
use dirs::home_dir;
use parking_lot::RwLock;

use super::object_storage::{get_object_store, AwsOptions, GcpOptions};

/// Wraps another catalog, automatically register require object stores for the file locations
#[derive(Debug)]
pub struct DynamicObjectStoreCatalog {
    inner: Arc<dyn CatalogProviderList>,
    state: Weak<RwLock<SessionState>>,
}

// impl DynamicObjectStoreCatalog {
//     pub fn new(inner: Arc<dyn CatalogProviderList>, state: Weak<RwLock<SessionState>>) -> Self {
//         Self { inner, state }
//     }
// }

impl CatalogProviderList for DynamicObjectStoreCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.inner.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let state = self.state.clone();
        self.inner
            .catalog(name)
            .map(|catalog| Arc::new(DynamicObjectStoreCatalogProvider::new(catalog, state)) as _)
    }
}

/// Wraps another catalog provider
#[derive(Debug)]
struct DynamicObjectStoreCatalogProvider {
    inner: Arc<dyn CatalogProvider>,
    state: Weak<RwLock<SessionState>>,
}

impl DynamicObjectStoreCatalogProvider {
    pub fn new(inner: Arc<dyn CatalogProvider>, state: Weak<RwLock<SessionState>>) -> Self {
        Self { inner, state }
    }
}

impl CatalogProvider for DynamicObjectStoreCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let state = self.state.clone();
        self.inner
            .schema(name)
            .map(|schema| Arc::new(DynamicObjectStoreSchemaProvider::new(schema, state)) as _)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }
}

/// Wraps another schema provider. [DynamicObjectStoreSchemaProvider] is responsible for registering the required
/// object stores for the file locations.
#[derive(Debug)]
struct DynamicObjectStoreSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    state: Weak<RwLock<SessionState>>,
}

impl DynamicObjectStoreSchemaProvider {
    pub fn new(inner: Arc<dyn SchemaProvider>, state: Weak<RwLock<SessionState>>) -> Self {
        Self { inner, state }
    }
}

#[async_trait]
impl SchemaProvider for DynamicObjectStoreSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let inner_table = self.inner.table(name).await;
        if inner_table.is_ok() {
            if let Some(inner_table) = inner_table? {
                return Ok(Some(inner_table));
            }
        }

        // if the inner schema provider didn't have a table by
        // that name, try to treat it as a listing table
        let mut state = self
            .state
            .upgrade()
            .ok_or_else(|| plan_datafusion_err!("locking error"))?
            .read()
            .clone();
        let mut builder = SessionStateBuilder::from(state.clone());
        let optimized_name = substitute_tilde(name.to_owned());
        let table_url = ListingTableUrl::parse(optimized_name.as_str())?;
        let scheme = table_url.scheme();
        let url = table_url.as_ref();

        // If the store is already registered for this URL then `get_store`
        // will return `Ok` which means we don't need to register it again. However,
        // if `get_store` returns an `Err` then it means the corresponding store is
        // not registered yet and we need to register it
        match state.runtime_env().object_store_registry.get_store(url) {
            Ok(_) => { /*Nothing to do here, store for this URL is already registered*/ }
            Err(_) => {
                // Register the store for this URL. Here we don't have access
                // to any command options so the only choice is to use an empty collection
                match scheme {
                    "s3" | "oss" | "cos" => {
                        if let Some(table_options) = builder.table_options() {
                            table_options.extensions.insert(AwsOptions::default())
                        }
                    }
                    "gs" | "gcs" => {
                        if let Some(table_options) = builder.table_options() {
                            table_options.extensions.insert(GcpOptions::default())
                        }
                    }
                    _ => {}
                };
                state = builder.build();
                let store = get_object_store(
                    &state,
                    table_url.scheme(),
                    url,
                    &state.default_table_options(),
                )
                .await?;
                state.runtime_env().register_object_store(url, store);
            }
        }
        self.inner.table(name).await
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

pub fn substitute_tilde(cur: String) -> String {
    if let Some(usr_dir_path) = home_dir() {
        if let Some(usr_dir) = usr_dir_path.to_str() {
            if cur.starts_with('~') && !usr_dir.is_empty() {
                return cur.replacen('~', usr_dir, 1);
            }
        }
    }
    cur
}
