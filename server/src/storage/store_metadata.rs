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

use bytes::Bytes;
use once_cell::sync::OnceCell;
use relative_path::RelativePathBuf;
use std::io;

use crate::{
    metadata::error::stream_info::MetadataError,
    option::{Mode, CONFIG, JOIN_COMMUNITY},
    rbac::{role::model::DefaultPrivilege, user::User},
    storage::ObjectStorageError,
    utils::uid,
};

use super::PARSEABLE_METADATA_FILE_NAME;

// Expose some static variables for internal usage
pub static STORAGE_METADATA: OnceCell<StaticStorageMetadata> = OnceCell::new();
pub const CURRENT_STORAGE_METADATA_VERSION: &str = "v4";
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
    pub server_mode: String,
    #[serde(default)]
    pub roles: HashMap<String, Vec<DefaultPrivilege>>,
    #[serde(default)]
    pub default_role: Option<String>,
    pub coordinator_endpoint: Option<String>
}

impl StorageMetadata {
    pub fn new() -> Self {
        let endpoint = match CONFIG.parseable.mode {
            // for Query and Ingest are we to assume that Coordinator Node already exists?
            Mode::Coordinator => {
                let url = format!(
                    "{}://{}",
                    CONFIG.parseable.get_scheme(),
                    CONFIG.parseable.address
                );
                Some(url)
            },
            _ => None,
        };
        Self {
            version: CURRENT_STORAGE_METADATA_VERSION.to_string(),
            mode: CONFIG.storage_name.to_owned(),
            staging: CONFIG.staging_dir().to_path_buf(),
            storage: CONFIG.storage().get_endpoint(),
            deployment_id: uid::gen(),
            server_mode: CONFIG.parseable.mode.to_string(),
            users: Vec::new(),
            streams: Vec::new(),
            roles: HashMap::default(),
            default_role: None,
            coordinator_endpoint: endpoint
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
pub async fn resolve_parseable_metadata(
    parseable_metadata: &Option<Bytes>,
) -> Result<StorageMetadata, ObjectStorageError> {
    let staging_metadata = get_staging_metadata()?;
    let mut remote_metadata: Option<StorageMetadata> = None;
    if parseable_metadata.is_some() {
        remote_metadata = Some(
            serde_json::from_slice(parseable_metadata.as_ref().unwrap())
                .expect("parseable config is valid json"),
        );
    }
    // Env Change needs to be updated
    let check = determine_environment(staging_metadata, remote_metadata);
    // flags for if metadata needs to be synced
    let mut overwrite_staging = false;
    let mut overwrite_remote = false;

    let res = match check {
        EnvChange::Illegal(remote, config) => {
            Err(format!("Migrating from {} mode to {} mode is not allowed.", remote,config))
        }
        EnvChange::None(remote) => {
            // overwrite staging anyways so that it matches remote in case of any divergence
            overwrite_staging = true;
            if CONFIG.parseable.mode ==  Mode::All {
                standalone_after_distributed(Mode::from_string(&remote.server_mode).expect("mode should be valid here"))
                    ?;
            }
            Ok(remote)
        },
        EnvChange::NewRemote => {
            Err("Could not start the server because staging directory indicates stale data from previous deployment, please choose an empty staging directory and restart the server".to_string())
        }
        EnvChange::NewStaging(mut remote) => {
            // if server is started in ingest/query mode, we need to make sure that coordinator mode has been started
            // i.e the metadata is updated to reflect the server mode = Coordinator
            if Mode::from_string(&remote.server_mode)
            .map_err(ObjectStorageError::Custom)
            ?
            != Mode::Coordinator && (CONFIG.parseable.mode == Mode::Ingest || CONFIG.parseable.mode == Mode::Query) {
                Err("Please make sure that Coordinator server starts up first".to_string())
            } else {
                create_dir_all(CONFIG.staging_dir())?;
                remote.staging = CONFIG.staging_dir().canonicalize()?;
                // this flag is set to true so that metadata is copied to staging
                overwrite_staging = true;
                // overwrite remote in all and coordinator mode
                // because staging dir has changed.
                match CONFIG.parseable.mode {
                    Mode::All => {
                        standalone_after_distributed(Mode::from_string(&remote.server_mode).expect("mode should be valid at here"))
                            .map_err(|err| {
                                ObjectStorageError::Custom(err.to_string())
                            })?;
                            overwrite_remote = true;
                    },
                    Mode::Coordinator => {
                        overwrite_remote = true;
                        remote.server_mode = CONFIG.parseable.mode.to_string();
                        remote.staging = CONFIG.staging_dir().to_path_buf();
                    },
                    Mode::Query => {
                        // if query server is started fetch the metadata from remote
                        // update the server mode for local metadata
                        remote.server_mode = CONFIG.parseable.mode.to_string();
                        remote.staging = CONFIG.staging_dir().to_path_buf();
                    },
                    Mode::Ingest => {
                        // if ingest server is started fetch the metadata from remote
                        // update the server mode for local metadata
                        remote.server_mode = CONFIG.parseable.mode.to_string();
                        remote.staging = CONFIG.staging_dir().to_path_buf();
                      },
                }
                Ok(remote)
            }
        }
        EnvChange::CreateBoth => {
            create_dir_all(CONFIG.staging_dir())?;
            let metadata = StorageMetadata::new();
            // new metadata needs to be set
            // if mode is coordinator or all then both staging and remote
            match CONFIG.parseable.mode {
                Mode::All | Mode::Coordinator => overwrite_remote = true,
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
        log::warn!("overwrite remote = true\nmetadata- {metadata:?}");
        put_remote_metadata(&metadata).await?;
    }

    if overwrite_staging {
        log::warn!("overwrite staging = true\nmetadata- {metadata:?}");
        put_staging_metadata(&metadata)?;
    }

    Ok(metadata)
}

fn determine_environment(
    staging_metadata: Option<StorageMetadata>,
    remote_metadata: Option<StorageMetadata>,
) -> EnvChange {
    log::warn!("staging_metadata- {staging_metadata:?}");
    log::warn!("remote_metadata- {remote_metadata:?}");
    match (staging_metadata, remote_metadata) {
        (Some(staging), Some(remote)) => {
            let remote_mode = Mode::from_string(&remote.server_mode).expect("server mode is valid here");
            // if both staging and remote have same deployment id
            if staging.deployment_id == remote.deployment_id {
                // even in this case we can have different server modes
                // check the server mode present in CONFIG and compare that to remote
                let config_mode = &CONFIG.parseable.mode;
                match (&remote_mode, config_mode) {
                    (Mode::Query, Mode::Coordinator) => EnvChange::NewStaging(remote), // migration
                    (Mode::All, Mode::Coordinator) => EnvChange::NewStaging(remote), // migration
                    (Mode::Ingest, Mode::Coordinator) => EnvChange::Illegal(remote_mode.to_string(), config_mode.to_string()), // migration
                    (Mode::Coordinator, Mode::Query) => EnvChange::None(remote), // restart
                    (Mode::Coordinator, Mode::Ingest) => EnvChange::None(remote), // restart
                    (Mode::Coordinator, Mode::Coordinator) => EnvChange::None(remote), // restart
                    (Mode::All, Mode::All) => EnvChange::None(remote), // restart
                    _ => EnvChange::Illegal(remote_mode.to_string(), config_mode.to_string())
                    // (Mode::Query, Mode::Query) => EnvChange::Illegal(remote_mode.to_string(), config_mode.to_string()),
                    // (Mode::Query, Mode::Ingest) => EnvChange::Illegal(remote_mode.to_string(), config_mode.to_string()),
                    // (Mode::Query, Mode::All) => EnvChange::Illegal(remote_mode.to_string(), config_mode.to_string()),
                    // (Mode::Ingest, Mode::Query) => EnvChange::Illegal(remote_mode.to_string(), config_mode.to_string()),
                    // (Mode::Ingest, Mode::Ingest) => EnvChange::Illegal(remote_mode.to_string(), config_mode.to_string()),
                    // (Mode::Ingest, Mode::All) => EnvChange::Illegal(remote_mode.to_string(), config_mode.to_string()),
                    // (Mode::Coordinator, Mode::All) => EnvChange::Illegal(remote_mode.to_string(), config_mode.to_string()),
                    // (Mode::All, Mode::Query) => EnvChange::Illegal(remote_mode.to_string(), config_mode.to_string()),
                    // (Mode::All, Mode::Ingest) => EnvChange::Illegal(remote_mode.to_string(), config_mode.to_string()),
                }
            } else if remote_mode == Mode::All
                && (CONFIG.parseable.mode == Mode::Query || CONFIG.parseable.mode == Mode::Ingest)
            {
                // if you are switching to distributed mode from standalone mode
                // it will create a new staging rather than a new remote
                EnvChange::NewStaging(remote)
            } else {
                // it is a new remote
                EnvChange::NewRemote
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
    /// Illegal migration
    Illegal(String, String)
}

fn standalone_after_distributed(remote_server_mode: Mode) -> Result<(), MetadataError> {
    // standalone -> coordinator | query | ingest allowed
    // but coordinator | query | ingest -> standalone not allowed
    if (remote_server_mode == Mode::Coordinator) || (remote_server_mode == Mode::Query) || (remote_server_mode == Mode::Ingest) {
        return Err(MetadataError::StandaloneWithDistributed("Starting Standalone Mode is not permitted when Distributed Mode is enabled. Please restart the server with Distributed Mode enabled.".to_string()));
    }

    Ok(())
}

pub fn get_staging_metadata() -> io::Result<Option<StorageMetadata>> {
    let path = RelativePathBuf::from(PARSEABLE_METADATA_FILE_NAME).to_path(CONFIG.staging_dir());
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
    let mut staging_metadata = meta.clone();
    staging_metadata.server_mode = CONFIG.parseable.mode.to_string();
    staging_metadata.staging = CONFIG.staging_dir().to_path_buf();
    let path = CONFIG.staging_dir().join(PARSEABLE_METADATA_FILE_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    serde_json::to_writer(&mut file, &staging_metadata)?;
    Ok(())
}
