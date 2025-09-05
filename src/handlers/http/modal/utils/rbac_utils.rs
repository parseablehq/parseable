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

use crate::{
    parseable::PARSEABLE,
    storage::{self, ObjectStorageError, StorageMetadata},
};

pub async fn get_metadata() -> Result<crate::storage::StorageMetadata, ObjectStorageError> {
    let metadata = PARSEABLE
        .metastore
        .get_parseable_metadata()
        .await
        .map_err(|e| ObjectStorageError::MetastoreError(Box::new(e.to_detail())))?
        .expect("metadata is initialized");
    Ok(serde_json::from_slice::<StorageMetadata>(&metadata)?)
}

pub async fn put_metadata(metadata: &StorageMetadata) -> Result<(), ObjectStorageError> {
    storage::put_remote_metadata(metadata).await?;
    storage::put_staging_metadata(metadata)?;
    Ok(())
}
