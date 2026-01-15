/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::{collections::HashSet, sync::Arc};

use dashmap::DashMap;
use itertools::Itertools;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::{rbac::role::Action, storage::StorageMetadata};

pub static TENANT_METADATA: Lazy<Arc<TenantMetadata>> =
    Lazy::new(|| Arc::new(TenantMetadata::default()));

#[derive(Default)]
pub struct TenantMetadata {
    tenants: DashMap<String, TenantOverview>,
}

#[derive(Default, PartialEq, Eq)]
pub struct TenantOverview {
    suspended_services: HashSet<Service>,
    meta: StorageMetadata,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum Service {
    Ingest,
    Query,
    Workspace,
}

impl TenantMetadata {
    pub fn insert_tenant(&self, tenant_id: String, meta: StorageMetadata) {
        let suspensions = meta.suspended_services.clone().unwrap_or_default();
        self.tenants.insert(
            tenant_id,
            TenantOverview {
                suspended_services: suspensions,
                meta,
            },
        );
    }

    pub fn suspend_service(&self, tenant_id: &str, service: Service) {
        if let Some(mut tenant) = self.tenants.get_mut(tenant_id) {
            tenant.suspended_services.insert(service);
        }
    }

    pub fn resume_service(&self, tenant_id: &str, service: Service) {
        if let Some(mut tenant) = self.tenants.get_mut(tenant_id) {
            tenant.suspended_services.remove(&service);
        }
    }

    pub fn delete_tenant(&self, tenant_id: &str) {
        self.tenants.remove(tenant_id);
    }

    pub fn is_action_suspended(
        &self,
        tenant_id: &str,
        action: &Action,
    ) -> Result<Option<String>, TenantNotFound> {
        if let Some(tenant) = self.tenants.get(tenant_id) {
            let states = &tenant.value().suspended_services;
            if states.contains(&Service::Ingest) && action.eq(&Action::Ingest) {
                Ok(Some("Ingestion is suspended for your workspace".into()))
            } else if states.contains(&Service::Query) && action.eq(&Action::Query) {
                Ok(Some("Querying is suspended for your workspace".into()))
            } else if states.contains(&Service::Workspace) {
                Ok(Some("Your workspace is suspended".into()))
            } else {
                Ok(None)
            }
        } else {
            return Err(TenantNotFound(tenant_id.to_owned()));
        }
    }

    pub fn get_tenants(&self) -> Vec<(String, StorageMetadata)> {
        self.tenants
            .iter()
            .map(|k| (k.key().clone(), k.value().meta.clone()))
            .collect_vec()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Tenant not found: {0}")]
pub struct TenantNotFound(pub String);

// // Type for serialization and deserialization
// #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
// pub struct TenantMetadata {
//     pub tenant_id: String,
//     pub version: String,
//     pub mode: String,
//     pub staging: PathBuf,
//     pub storage: String,
//     #[serde(default = "crate::utils::uid::generate_ulid")]
//     pub deployment_id: uid::Uid,
//     pub users: Vec<User>,
//     pub user_groups: Vec<UserGroup>,
//     pub streams: Vec<String>,
//     pub server_mode: Mode,
//     #[serde(default)]
//     pub roles: HashMap<String, Vec<DefaultPrivilege>>,
//     #[serde(default)]
//     pub default_role: Option<String>,
// }

// impl TenantMetadata {
//     pub fn from_storage_meta(meta: StorageMetadata, id: &str) -> Self {
//         Self {
//             tenant_id: id.to_owned(),
//             version: meta.version,
//             mode: meta.mode,
//             staging: meta.staging,
//             storage: meta.storage,
//             deployment_id: meta.deployment_id,
//             users: meta.users,
//             user_groups: meta.user_groups,
//             streams: meta.streams,
//             server_mode: meta.server_mode,
//             roles: meta.roles,
//             default_role: meta.default_role,
//         }
//     }
// }

// impl MetastoreObject for TenantMetadata {
//     fn get_object_path(&self) -> String {
//         format!("{}/.parseable.json", &self.tenant_id)
//     }

//     fn get_object_id(&self) -> String {
//         self.tenant_id.clone()
//     }
// }
