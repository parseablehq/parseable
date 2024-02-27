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

use std::{
    collections::HashMap,
    fs::{self, create_dir_all, OpenOptions},
    path::PathBuf,
};

use once_cell::sync::OnceCell;
use std::io;

use crate::{
    option::{Mode, CONFIG, JOIN_COMMUNITY},
    rbac::{role::model::DefaultPrivilege, user::User},
    storage::ObjectStorageError,
    utils::uid,
};

use super::PARSEABLE_METADATA_FILE_NAME;

// Expose some static variables for internal usage
pub static STORAGE_METADATA: OnceCell<StaticStorageMetadata> = OnceCell::new();

// For use in global static
#[derive(Debug, PartialEq, Eq)]
pub struct StaticStorageMetadata {
    pub mode: String,
    pub deployment_id: uid::Uid,
}

// Type for serialization and deserialization
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StorageMetadata {
    pub version: String,
    pub mode: String,
    pub staging: PathBuf,
    pub storage: String,
    #[serde(default = "crate::utils::uid::gen")]
    pub deployment_id: uid::Uid,
    pub users: Vec<User>,
    pub streams: Vec<String>,
    #[serde(default)]
    pub roles: HashMap<String, Vec<DefaultPrivilege>>,
    #[serde(default)]
    pub default_role: Option<String>,
}

impl StorageMetadata {
    pub fn new() -> Self {
        Self {
            version: "v3".to_string(),
            mode: CONFIG.storage_name.to_owned(),
            staging: CONFIG.staging_dir().to_path_buf(),
            storage: CONFIG.storage().get_endpoint(),
            deployment_id: uid::gen(),
            users: Vec::new(),
            streams: Vec::new(),
            roles: HashMap::default(),
            default_role: None,
        }
    }

    pub fn global() -> &'static StaticStorageMetadata {
        STORAGE_METADATA
            .get()
            .expect("gloabal static is initialized")
    }

    pub fn set_global(self) {
        let metadata = StaticStorageMetadata {
            mode: self.mode,
            deployment_id: self.deployment_id,
        };

        STORAGE_METADATA.set(metadata).expect("only set once")
    }
}

/// deals with the staging directory creation and metadata resolution
/// always returns remote metadata as it is source of truth
/// overwrites staging metadata while updating storage info
pub async fn resolve_parseable_metadata() -> Result<StorageMetadata, ObjectStorageError> {
    let staging_metadata = get_staging_metadata()?;
    let storage = CONFIG.storage().get_object_store();
    let remote_metadata = storage.get_metadata().await?;

    let check = match (staging_metadata, remote_metadata) {
        (Some(staging), Some(remote)) => {
            if staging.deployment_id == remote.deployment_id {
                EnvChange::None(remote)
            } else {
                EnvChange::NewRemote
            }
        }
        (None, Some(remote)) => EnvChange::NewStaging(remote),
        (Some(_), None) => EnvChange::NewRemote,
        (None, None) => EnvChange::CreateBoth,
    };

    // flags for if metadata needs to be synced
    let mut overwrite_staging = false;
    let mut overwrite_remote = false;

    let res = match check {
        EnvChange::None(metadata) => {
            // overwrite staging anyways so that it matches remote in case of any divergence
            overwrite_staging = true;
            Ok(metadata)
        },
        EnvChange::NewRemote => {
            Err("Could not start the server because staging directory indicates stale data from previous deployment, please choose an empty staging directory and restart the server")
        }
        EnvChange::NewStaging(mut metadata) => {
            create_dir_all(CONFIG.staging_dir())?;
            metadata.staging = CONFIG.staging_dir().canonicalize()?;
            // this flag is set to true so that metadata is copied to staging
            overwrite_staging = true;
            // overwrite remote in all and query mode
            // because staging dir has changed.
            match CONFIG.parseable.mode {
                Mode::All | Mode::Query => overwrite_remote = true,
                _ => {
                    metadata.staging = CONFIG.staging_dir().to_path_buf();
                },
            }
            Ok(metadata)
        }
        EnvChange::CreateBoth => {
            create_dir_all(CONFIG.staging_dir())?;
            let metadata = StorageMetadata::new();
            // new metadata needs to be set
            // if mode is query or all then both staging and remote
            match CONFIG.parseable.mode {
                Mode::All | Mode::Query => overwrite_remote = true,
                _ => (),
            }
            // else only staging
            overwrite_staging = true;
            Ok(metadata)
        }
    };

    let metadata = res.map_err(|err| {
        let err = format!("{}. {}", err, JOIN_COMMUNITY);
        let err: Box<dyn std::error::Error + Send + Sync + 'static> = err.into();
        ObjectStorageError::UnhandledError(err)
    })?;

    if overwrite_remote {
        put_remote_metadata(&metadata).await?;
    }

    if overwrite_staging {
        put_staging_metadata(&metadata)?;
    }

    Ok(metadata)
}

// variant contain remote metadata
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvChange {
    /// No change in env i.e both staging and remote have same id
    /// or deployment id of staging is not matching with that of remote
    None(StorageMetadata),
    /// Metadata not found in storage. Treated as possible misconfiguration on user side.
    NewRemote,
    /// If a new staging is found then we just copy remote metadata to this staging.
    NewStaging(StorageMetadata),
    /// Fresh remote and staging, hence create a new metadata file on both
    CreateBoth,
}

pub fn get_staging_metadata() -> io::Result<Option<StorageMetadata>> {
    let path = CONFIG.staging_dir().join(PARSEABLE_METADATA_FILE_NAME);
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) => match err.kind() {
            io::ErrorKind::NotFound => return Ok(None),
            _ => return Err(err),
        },
    };

    let meta: StorageMetadata = serde_json::from_slice(&bytes).unwrap();

    Ok(Some(meta))
}

pub async fn put_remote_metadata(metadata: &StorageMetadata) -> Result<(), ObjectStorageError> {
    let client = CONFIG.storage().get_object_store();
    client.put_metadata(metadata).await
}

pub fn put_staging_metadata(meta: &StorageMetadata) -> io::Result<()> {
    let path = CONFIG.staging_dir().join(PARSEABLE_METADATA_FILE_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    serde_json::to_writer(&mut file, meta)?;
    Ok(())
}
