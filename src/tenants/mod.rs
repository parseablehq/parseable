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

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use dashmap::DashMap;
use once_cell::sync::Lazy;

use crate::{
    metastore::metastore_traits::MetastoreObject,
    option::Mode,
    rbac::{
        role::model::DefaultPrivilege,
        user::{User, UserGroup},
    },
    storage::StorageMetadata,
    utils::uid,
};

pub static TENANT_METADATA: Lazy<Arc<DashMap<String, StorageMetadata>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

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
