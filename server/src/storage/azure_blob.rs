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
use object_store::azure::MicrosoftAzureBuilder;
use super::s3::{ObjStoreClient, CONNECT_TIMEOUT_SECS, REQUEST_TIMEOUT_SECS};
use super::ObjectStorageProvider;
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::datasource::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl,
};
use crate::metrics::storage::StorageMetrics;
use std::sync::Arc;
use object_store::limit::LimitStore;
use super::metrics_layer::MetricLayer;
use object_store::path::Path as StorePath;
use object_store::ClientOptions;
use std::time::Duration;


#[derive(Debug, Clone, clap::Args)]
#[command(
    name = "Azure config",
    about = "Start Parseable with Azure Blob storage",
    help_template = "\
{about-section}
{all-args}
"
)]
pub struct AzureBlobConfig {
    // The Azure Storage Account ID
    #[arg(long, env = "P_AZR_URL", value_name = "url", required = false)]
    pub url: String,
    
    // The Azure Storage Account ID
    #[arg(long, env = "P_AZR_ACCOUNT", value_name = "account", required = true)]
    pub account: String,

    /// The Azure Storage Access key
    #[arg(long, env = "P_AZR_ACCESS_KEY", value_name = "access-key", required = true)]
    pub access_key: String,

    /// The container name to be used for storage
    #[arg(long, env = "P_AZR_CONTAINER", value_name = "container", required = true)]
    pub container: String,
}

impl AzureBlobConfig {
    fn get_default_builder(&self) -> MicrosoftAzureBuilder {
        let client_options = ClientOptions::default()
        .with_allow_http(true)
        .with_connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
        .with_timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS));

        let builder = MicrosoftAzureBuilder::new()
        .with_endpoint(self.url.clone())
        .with_account(self.account.clone())
        .with_access_key(self.access_key.clone())
        .with_container_name(self.container.clone());

        return builder.with_client_options(client_options)
    }
}

impl ObjectStorageProvider for AzureBlobConfig {
    fn get_datafusion_runtime(&self) -> RuntimeConfig {
        let azure = self.get_default_builder().build().unwrap();
        // limit objectstore to a concurrent request limit
        let azure = LimitStore::new(azure, super::MAX_OBJECT_STORE_REQUESTS);
        let azure = MetricLayer::new(azure);

        let object_store_registry: DefaultObjectStoreRegistry = DefaultObjectStoreRegistry::new();
        let url = ObjectStoreUrl::parse(format!("az://{}", &self.container)).unwrap();
        object_store_registry.register_store(url.as_ref(), Arc::new(azure));

        RuntimeConfig::new().with_object_store_registry(Arc::new(object_store_registry))
    }

    fn get_object_store(&self) -> Arc<dyn super::ObjectStorage + Send> {
        let azure = self.get_default_builder().build().unwrap();

        // limit objectstore to a concurrent request limit
        let azure = LimitStore::new(azure, super::MAX_OBJECT_STORE_REQUESTS);
        Arc::new(ObjStoreClient::new(azure, self.container.clone(), StorePath::from("")))
    }

    fn get_endpoint(&self) -> String {
        return self.url.clone()
    }

    fn register_store_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
        self.register_metrics(handler)
    }
}
