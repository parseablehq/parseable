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
    io::Write,
    path::PathBuf,
};

use once_cell::sync::OnceCell;
use std::io;

use crate::{option::CONFIG, utils::uid};

use super::{encryption, object_storage::PARSEABLE_METADATA_FILE_NAME};

pub const CURRENT_STORAGE_METADATA_VERSION: &str = "v2";
pub const CURRENT_ENCRYPTION_ALGORITHM: &str = "AES-GCM-SIV";

pub static STORAGE_METADATA: OnceCell<StorageMetadata> = OnceCell::new();

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StorageMetadata {
    pub version: String,
    pub encryption_algorithm: String,
    pub mode: String,
    pub staging: PathBuf,
    pub storage: String,
    #[serde(default = "crate::utils::uid::gen")]
    pub deployment_id: uid::Uid,
    pub user: Vec<User>,
    pub stream: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct User {
    username: String,
    password: String,
    role: String,
}

impl StorageMetadata {
    pub fn new() -> Self {
        Self {
            version: CURRENT_STORAGE_METADATA_VERSION.to_string(),
            mode: CONFIG.storage_name.to_owned(),
            encryption_algorithm: CURRENT_ENCRYPTION_ALGORITHM.to_string(),
            staging: CONFIG.staging_dir().canonicalize().unwrap(),
            storage: CONFIG.storage().get_endpoint(),
            deployment_id: uid::gen(),
            user: Vec::new(),
            stream: Vec::new(),
        }
    }

    pub fn global() -> &'static Self {
        STORAGE_METADATA
            .get()
            .expect("gloabal static is initialized")
    }

    pub fn set_global(self) {
        STORAGE_METADATA.set(self).expect("only set once")
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

    let bytes = encryption::decrypt(encryption::key().as_bytes(), bytes.into());
    let meta: StorageMetadata = serde_json::from_slice(&bytes).unwrap();

    Ok(Some(meta))
}

pub fn put_staging_metadata(meta: &StorageMetadata) -> io::Result<()> {
    let path = CONFIG.staging_dir().join(PARSEABLE_METADATA_FILE_NAME);
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    let bytes = serde_json::to_vec(meta).expect("serializable");
    let bytes = encryption::encrypt(encryption::key().as_bytes(), bytes.into());
    file.write_all(&bytes)
}
