use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use ulid::Ulid;

use crate::{
    alerts::AlertConfig,
    catalog::{manifest::Manifest, snapshot::Snapshot},
    event::format::LogSourceEntry,
    handlers::http::modal::NodeMetadata,
    rbac::{role::model::DefaultPrivilege, user::User},
    stats::FullStats,
    storage::{ObjectStoreFormat, retention::Retention, StreamType, StorageMetadata},
};

#[derive(Debug, thiserror::Error)]
pub enum MetastoreError {
    #[error("Connection failed: {0}")]
    Connection(String),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Conflict: {0}")]
    Conflict(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

pub trait MetaStoreProvider: std::fmt::Debug + Send + Sync{
    fn get_meta_store(&self) -> Arc<dyn MetaStorage>{
        static META_STORE: OnceCell<Arc<dyn MetaStorage>> = OnceCell::new();

        META_STORE.get_or_init(|| self.construct_client()).clone()
    }
    fn construct_client(&self) -> Arc<dyn MetaStorage>;
    fn get_endpoint(&self) -> String;
    fn register_storage_metrics(&self, handler: &PrometheusMetrics);
    fn name(&self) -> &'static str;
}

#[async_trait]
pub trait Metastore: Send + Sync + std::fmt::Debug {
    async fn initialize(&self) -> Result<(), MetastoreError>;
    
    async fn health_check(&self) -> Result<(), MetastoreError>;

    async fn upsert_stream_metadata(
        &self, 
        stream_name: &str, 
        metadata: &ObjectStoreFormat
    ) -> Result<(), MetastoreError>;
    
    async fn get_stream_metadata(
        &self, 
        stream_name: &str
    ) -> Result<Option<ObjectStoreFormat>, MetastoreError>;
    
    async fn list_streams(&self) -> Result<Vec<String>, MetastoreError>;
    
    async fn delete_stream_metadata(&self, stream_name: &str) -> Result<(), MetastoreError>;
    
    async fn update_stream_stats(
        &self, 
        stream_name: &str, 
        stats: &FullStats
    ) -> Result<(), MetastoreError>;
    
    async fn update_stream_snapshot(
        &self, 
        stream_name: &str, 
        snapshot: &Snapshot
    ) -> Result<(), MetastoreError>;
    
    async fn update_stream_retention(
        &self, 
        stream_name: &str, 
        retention: &Retention
    ) -> Result<(), MetastoreError>;

    async fn update_first_event(
        &self, 
        stream_name: &str, 
        first_event: &str
    ) -> Result<(), MetastoreError>;

    async fn update_log_source(
        &self, 
        stream_name: &str, 
        log_source: &[LogSourceEntry]
    ) -> Result<(), MetastoreError>;

    async fn put_schema(
        &self, 
        stream_name: &str, 
        schema: &arrow_schema::Schema
    ) -> Result<(), MetastoreError>;
    
    async fn get_schema(
        &self, 
        stream_name: &str
    ) -> Result<Option<arrow_schema::Schema>, MetastoreError>;

    async fn upsert_node(&self, node: &NodeMetadata) -> Result<(), MetastoreError>;
    
    async fn get_node(&self, node_id: &str) -> Result<Option<NodeMetadata>, MetastoreError>;
    
    async fn list_nodes_by_type(&self, node_type: &str) -> Result<Vec<NodeMetadata>, MetastoreError>;
    
    async fn remove_node(&self, node_id: &str) -> Result<(), MetastoreError>;
    
    async fn update_node_heartbeat(&self, node_id: &str) -> Result<(), MetastoreError>;

    async fn put_global_metadata(&self, metadata: &StorageMetadata) -> Result<(), MetastoreError>;
    
    async fn get_global_metadata(&self) -> Result<Option<StorageMetadata>, MetastoreError>;

    async fn upsert_user(&self, user: &User) -> Result<(), MetastoreError>;
    
    async fn get_user(&self, username: &str) -> Result<Option<User>, MetastoreError>;
    
    async fn list_users(&self) -> Result<Vec<User>, MetastoreError>;
    
    async fn delete_user(&self, username: &str) -> Result<(), MetastoreError>;

    async fn put_alert(&self, alert_id: Ulid, alert: &AlertConfig) -> Result<(), MetastoreError>;
    
    async fn get_alert(&self, alert_id: Ulid) -> Result<Option<AlertConfig>, MetastoreError>;
    
    async fn list_alerts(&self) -> Result<Vec<AlertConfig>, MetastoreError>;
    
    async fn delete_alert(&self, alert_id: Ulid) -> Result<(), MetastoreError>;

    async fn put_manifest(
        &self, 
        stream_name: &str, 
        date_prefix: &str, 
        manifest: &Manifest
    ) -> Result<(), MetastoreError>;
    
    async fn get_manifest(
        &self, 
        stream_name: &str, 
        date_prefix: &str
    ) -> Result<Option<Manifest>, MetastoreError>;

    async fn put_dashboard(
        &self, 
        user_id: &str, 
        dashboard_id: &str, 
        data: &Bytes
    ) -> Result<(), MetastoreError>;
    
    async fn get_dashboard(
        &self, 
        user_id: &str, 
        dashboard_id: &str
    ) -> Result<Option<Bytes>, MetastoreError>;
    
    async fn put_filter(
        &self, 
        user_id: &str, 
        stream_name: &str, 
        filter_id: &str, 
        data: &Bytes
    ) -> Result<(), MetastoreError>;
    
    async fn get_filter(
        &self, 
        user_id: &str, 
        stream_name: &str, 
        filter_id: &str
    ) -> Result<Option<Bytes>, MetastoreError>;
}

pub trait MetaStorage: Debug + Send + Sync + 'static {
    
}
