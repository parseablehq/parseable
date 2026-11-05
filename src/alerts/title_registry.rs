use bytes::Bytes;
use relative_path::RelativePathBuf;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{alerts::AlertError, parseable::PARSEABLE, storage::object_storage::target_json_path};

fn path(title: &str, tenant_id: &Option<String>) -> RelativePathBuf {
    let parent = target_json_path(&Ulid::nil(), tenant_id);
    let digest = hex::encode(Sha256::digest(title.as_bytes()));
    parent
        .parent()
        .unwrap()
        .join(format!(".titles/{digest}.id"))
}

/// Reserve a title in shared storage before creating its ID-based object.
pub async fn reserve(
    title: &str,
    id: Ulid,
    tenant_id: &Option<String>,
) -> Result<bool, AlertError> {
    let storage = PARSEABLE.storage.get_object_store();
    Ok(storage
        .put_object_if_absent(
            &path(title, tenant_id),
            Bytes::from(id.to_string()),
            tenant_id,
        )
        .await?)
}

/// Release only the reservation owned by this object.
pub async fn release(title: &str, id: Ulid, tenant_id: &Option<String>) -> Result<(), AlertError> {
    let storage = PARSEABLE.storage.get_object_store();
    let path = path(title, tenant_id);
    match storage.get_object(&path, tenant_id).await {
        Ok(owner) if owner.as_ref() == id.to_string().as_bytes() => {
            storage.delete_object(&path, tenant_id).await?;
        }
        Ok(_) => {}
        Err(crate::storage::ObjectStorageError::NoSuchKey(_)) => {}
        Err(crate::storage::ObjectStorageError::IoError(error))
            if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}
