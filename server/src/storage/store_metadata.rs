use std::{
    fs::{self, OpenOptions},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use std::io;

use crate::{option::CONFIG, utils::hostname_unchecked};

use super::{object_storage::PARSEABLE_METADATA_FILE_NAME, ObjectStorageError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMetadata {
    pub version: String,
    pub mode: String,
    pub staging: PathBuf,
    pub storage: String,
    pub deployment_id: String,
    pub user: Vec<User>,
    pub stream: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    username: String,
    password: String,
    role: String,
}

impl StorageMetadata {
    pub fn new() -> Self {
        Self {
            version: "v1".to_string(),
            mode: CONFIG.storage_name.to_owned(),
            staging: CONFIG.staging_dir().canonicalize().unwrap(),
            storage: CONFIG.storage().get_endpoint(),
            deployment_id: hostname_unchecked(),
            user: Vec::new(),
            stream: Vec::new(),
        }
    }
}

pub async fn startup_check() -> Result<EnvChange, ObjectStorageError> {
    let staging_metadata = get_staging_metadata()?;
    let storage = CONFIG.storage().get_object_store();
    let remote_metadata = storage.get_metadata().await?;

    Ok(check_metadata_conflict(staging_metadata, remote_metadata))
}

fn check_metadata_conflict(
    staging_metadata: Option<StorageMetadata>,
    remote_metadata: Option<StorageMetadata>,
) -> EnvChange {
    match (staging_metadata, remote_metadata) {
        (Some(staging), Some(remote)) if staging.mode == remote.mode => {
            if staging.storage != remote.storage {
                EnvChange::StorageMismatch
            } else if staging.staging != remote.staging {
                EnvChange::StagingMismatch
            } else {
                EnvChange::None
            }
        }
        (Some(staging), Some(remote)) if staging.mode != remote.mode => EnvChange::StorageMismatch,
        (None, None) => EnvChange::CreateBoth,
        (None, Some(_)) => EnvChange::NewStaging,
        (Some(_), None) => EnvChange::NewRemote,
        _ => unreachable!(),
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvChange {
    None,
    StagingMismatch,
    StorageMismatch,
    NewRemote,
    NewStaging,
    CreateBoth,
}

fn get_staging_metadata() -> io::Result<Option<StorageMetadata>> {
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

pub fn put_staging_metadata(meta: &StorageMetadata) -> io::Result<()> {
    let path = CONFIG.staging_dir().join(PARSEABLE_METADATA_FILE_NAME);
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer(&mut file, meta)?;
    Ok(())
}
