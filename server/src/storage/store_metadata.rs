/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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
    fs::{self, OpenOptions},
    path::PathBuf,
};

use once_cell::sync::OnceCell;
use std::io;

use crate::{option::CONFIG, rbac::user::User, utils::uid};

use super::object_storage::PARSEABLE_METADATA_FILE_NAME;

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
    pub user: Vec<User>,
    pub stream: Vec<String>,
}

impl StorageMetadata {
    pub fn new() -> Self {
        Self {
            version: "v1".to_string(),
            mode: CONFIG.storage_name.to_owned(),
            staging: CONFIG.staging_dir().canonicalize().unwrap(),
            storage: CONFIG.storage().get_endpoint(),
            deployment_id: uid::gen(),
            user: Vec::new(),
            stream: Vec::new(),
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

pub fn check_metadata_conflict(
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
                EnvChange::None(staging)
            }
        }
        (Some(staging), Some(remote)) if staging.mode != remote.mode => EnvChange::StorageMismatch,
        (None, None) => EnvChange::CreateBoth,
        (None, Some(remote)) => EnvChange::NewStaging(remote),
        (Some(_), None) => EnvChange::NewRemote,
        _ => unreachable!(),
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvChange {
    None(StorageMetadata),
    StagingMismatch,
    StorageMismatch,
    NewRemote,
    NewStaging(StorageMetadata),
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

pub fn put_staging_metadata(meta: &StorageMetadata) -> io::Result<()> {
    let path = CONFIG.staging_dir().join(PARSEABLE_METADATA_FILE_NAME);
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer(&mut file, meta)?;
    Ok(())
}
