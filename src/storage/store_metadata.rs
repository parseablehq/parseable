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
    fs::{self, OpenOptions, create_dir_all},
    path::PathBuf,
};

use bytes::Bytes;
use once_cell::sync::OnceCell;
use relative_path::RelativePathBuf;
use std::io;

use crate::{
    option::Mode,
    parseable::{JOIN_COMMUNITY, PARSEABLE},
    rbac::{
        role::model::DefaultPrivilege,
        user::{User, UserGroup},
    },
    storage::ObjectStorageError,
    utils::uid,
};

use super::PARSEABLE_METADATA_FILE_NAME;

// Expose some static variables for internal usage
pub static STORAGE_METADATA: OnceCell<StaticStorageMetadata> = OnceCell::new();
pub const CURRENT_STORAGE_METADATA_VERSION: &str = "v6";
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
    #[serde(default = "crate::utils::uid::generate_ulid")]
    pub deployment_id: uid::Uid,
    pub users: Vec<User>,
    pub user_groups: Vec<UserGroup>,
    pub streams: Vec<String>,
    pub server_mode: Mode,
    #[serde(default)]
    pub roles: HashMap<String, Vec<DefaultPrivilege>>,
    #[serde(default)]
    pub default_role: Option<String>,
}

impl Default for StorageMetadata {
    fn default() -> Self {
        Self {
            version: CURRENT_STORAGE_METADATA_VERSION.to_string(),
            mode: PARSEABLE.storage.name().to_owned(),
            staging: PARSEABLE.options.staging_dir().to_path_buf(),
            storage: PARSEABLE.storage.get_endpoint(),
            deployment_id: uid::generate_ulid(),
            server_mode: PARSEABLE.options.mode,
            users: Vec::new(),
            user_groups: Vec::new(),
            streams: Vec::new(),
            roles: HashMap::default(),
            default_role: None,
        }
    }
}

impl StorageMetadata {
    pub fn global() -> &'static StaticStorageMetadata {
        STORAGE_METADATA
            .get()
            .expect("global static is initialized")
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
pub async fn resolve_parseable_metadata(
    parseable_metadata: &Option<Bytes>,
) -> Result<StorageMetadata, ObjectStorageError> {
    let staging_metadata = get_staging_metadata()?;
    let remote_metadata = parseable_metadata
        .as_ref()
        .map(|meta| serde_json::from_slice(meta).expect("parseable config is valid json"));

    let env_change = determine_environment(staging_metadata, remote_metadata);

    let (mut metadata, overwrite_staging, overwrite_remote) = process_env_change(env_change)?;

    metadata.server_mode = PARSEABLE.options.mode;

    if overwrite_remote {
        put_remote_metadata(&metadata).await?;
    }
    if overwrite_staging {
        put_staging_metadata(&metadata)?;
    }

    Ok(metadata)
}

fn process_env_change(
    env_change: EnvChange,
) -> Result<(StorageMetadata, bool, bool), ObjectStorageError> {
    match env_change {
        EnvChange::None(mut metadata) => handle_none_env(&mut metadata),
        EnvChange::NewRemote => handle_new_remote_env(),
        EnvChange::NewStaging(mut metadata) => handle_new_staging_env(&mut metadata),
        EnvChange::CreateBoth => handle_create_both_env(),
    }
}

fn handle_none_env(
    metadata: &mut StorageMetadata,
) -> Result<(StorageMetadata, bool, bool), ObjectStorageError> {
    let overwrite_staging = true;
    let mut overwrite_remote = false;

    match PARSEABLE.options.mode {
        Mode::All => {
            metadata.server_mode.standalone_after_distributed()?;
            overwrite_remote = true;
            update_metadata_mode_and_staging(metadata);
        }
        Mode::Query => {
            overwrite_remote = true;
            update_metadata_mode_and_staging(metadata);
        }
        _ => {}
    }
    if PARSEABLE.options.mode == Mode::All {
        metadata.server_mode.standalone_after_distributed()?;
    }
    Ok((metadata.clone(), overwrite_staging, overwrite_remote))
}

fn handle_new_remote_env() -> Result<(StorageMetadata, bool, bool), ObjectStorageError> {
    Err(ObjectStorageError::UnhandledError(format!(
        "Could not start the server because staging directory indicates stale data from previous deployment, please choose an empty staging directory and restart the server. {JOIN_COMMUNITY}"
    ).into()))
}

fn handle_new_staging_env(
    metadata: &mut StorageMetadata,
) -> Result<(StorageMetadata, bool, bool), ObjectStorageError> {
    if metadata.server_mode == Mode::All && PARSEABLE.options.mode == Mode::Ingest {
        return Err(ObjectStorageError::UnhandledError(
            format!(
            "Starting Ingest Mode is not allowed, Since Query Server has not been started yet. {JOIN_COMMUNITY}"
        )
            .into(),
        ));
    }
    create_dir_all(PARSEABLE.options.staging_dir())?;
    metadata.staging = PARSEABLE.options.staging_dir().canonicalize()?;
    let overwrite_staging = true;
    let mut overwrite_remote = false;

    match PARSEABLE.options.mode {
        Mode::All => {
            metadata
                .server_mode
                .standalone_after_distributed()
                .map_err(|err| ObjectStorageError::Custom(err.to_string()))?;
            overwrite_remote = true;
        }
        Mode::Query | Mode::Prism | Mode::Ingest | Mode::Index => {
            update_metadata_mode_and_staging(metadata);
            if matches!(PARSEABLE.options.mode, Mode::Query | Mode::Prism) {
                overwrite_remote = true;
            }
        }
    }
    Ok((metadata.clone(), overwrite_staging, overwrite_remote))
}

fn handle_create_both_env() -> Result<(StorageMetadata, bool, bool), ObjectStorageError> {
    create_dir_all(PARSEABLE.options.staging_dir())?;
    let metadata = StorageMetadata::default();
    let overwrite_remote = matches!(
        PARSEABLE.options.mode,
        Mode::All | Mode::Query | Mode::Prism
    );
    let overwrite_staging = true;
    Ok((metadata, overwrite_staging, overwrite_remote))
}

fn update_metadata_mode_and_staging(metadata: &mut StorageMetadata) {
    metadata.server_mode = PARSEABLE.options.mode;
    metadata.staging = PARSEABLE.options.staging_dir().to_path_buf();
}

pub fn determine_environment(
    staging_metadata: Option<StorageMetadata>,
    remote_metadata: Option<StorageMetadata>,
) -> EnvChange {
    match (staging_metadata, remote_metadata) {
        (Some(staging), Some(remote)) => {
            // if both staging and remote have same deployment id but different server modes
            if staging.deployment_id == remote.deployment_id
                && remote.server_mode == Mode::All
                && matches!(PARSEABLE.options.mode, Mode::Query | Mode::Ingest)
            {
                EnvChange::NewStaging(remote)
            } else if staging.deployment_id != remote.deployment_id {
                // if deployment id is different
                EnvChange::NewRemote
            } else {
                // if deployment id is same
                EnvChange::None(remote)
            }
        }
        (None, Some(remote)) => EnvChange::NewStaging(remote),
        (Some(_), None) => EnvChange::NewRemote,
        (None, None) => EnvChange::CreateBoth,
    }
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
    let path = RelativePathBuf::from(PARSEABLE_METADATA_FILE_NAME)
        .to_path(PARSEABLE.options.staging_dir());
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
    let client = PARSEABLE.storage.get_object_store();
    client.put_metadata(metadata).await
}

pub fn put_staging_metadata(meta: &StorageMetadata) -> io::Result<()> {
    let mut staging_metadata = meta.clone();
    staging_metadata.server_mode = PARSEABLE.options.mode;
    staging_metadata.staging = PARSEABLE.options.staging_dir().to_path_buf();
    let path = PARSEABLE
        .options
        .staging_dir()
        .join(PARSEABLE_METADATA_FILE_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    serde_json::to_writer(&mut file, &staging_metadata)?;
    Ok(())
}
