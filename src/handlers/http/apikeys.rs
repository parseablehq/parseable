use std::collections::HashSet;

use actix_web::{HttpRequest, HttpResponse, Responder, web};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, de};
use ulid::Ulid;

use crate::{
    apikeys::{
        ApiKeyError, ApiKeyType, CreateApiKeyRequest, validate_api_key as validate_api_key_value,
    },
    handlers::http::{
        modal::utils::rbac_utils::{get_metadata, put_metadata},
        rbac::{RBACError, UPDATE_LOCK},
    },
    parseable::DEFAULT_TENANT,
    rbac::{
        GLOBAL_INGESTOR_ROLE, GLOBAL_QUERIER_ROLE, Users,
        map::{roles, users},
        role::model::{DefaultPrivilege, Role},
        user::{User, UserType},
    },
    utils::{get_user_and_tenant_from_request, is_admin},
};

/// Verify the caller is an admin of their tenant or a super-admin.
fn verify_admin(req: &HttpRequest) -> Result<(String, Option<String>), ApiKeyError> {
    let (userid, tenant_id) = get_user_and_tenant_from_request(req)
        .map_err(|_| ApiKeyError::Unauthorized("Missing user identity".into()))?;

    if is_admin(req).map_err(|err| ApiKeyError::Unauthorized(err.to_string()))? {
        return Ok((userid, tenant_id));
    }

    Err(ApiKeyError::Unauthorized(
        "Only admins can manage API keys".into(),
    ))
}

/// Generate a fresh, opaque API key value as a UUID v4 string
/// (matches the original api key format used by CreateApiKeyRequest clients).
fn generate_api_key_value() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Serialisable shape returned by create / get.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiKeyResponse<'a> {
    key_id: Ulid,
    api_key: &'a str,
    key_name: &'a str,
    roles: &'a HashSet<String>,
    created_by: &'a str,
    created_at: DateTime<Utc>,
    modified_at: DateTime<Utc>,
}

impl<'a> TryFrom<&'a User> for ApiKeyResponse<'a> {
    type Error = ApiKeyError;

    fn try_from(user: &'a User) -> Result<Self, Self::Error> {
        let api_key = user
            .as_api_key()
            .ok_or_else(|| ApiKeyError::KeyNotFound(user.userid().to_string()))?;
        Ok(Self {
            key_id: api_key.key_id,
            api_key: &api_key.api_key,
            key_name: &api_key.key_name,
            roles: &user.roles,
            created_by: &api_key.created_by,
            created_at: api_key.created_at,
            modified_at: api_key.modified_at,
        })
    }
}

/// Serialisable shape returned by list (api_key value masked).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiKeyListEntry {
    key_id: Ulid,
    api_key: String,
    key_name: String,
    roles: HashSet<String>,
    created_by: String,
    created_at: DateTime<Utc>,
    modified_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ValidateApiKeyResponse {
    valid: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidateApiKeyRequest {
    api_key: String,
}

#[derive(Debug, Deserialize)]
pub struct ListApiKeysQuery {
    #[serde(default, deserialize_with = "deserialize_api_key_privilege")]
    priviledge: Option<Vec<DefaultPrivilege>>,
}

fn deserialize_api_key_privilege<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<DefaultPrivilege>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|privileges| {
            privileges
                .split(',')
                .map(str::trim)
                .map(|privilege| match privilege {
                    "reader" => Ok(DefaultPrivilege::Reader { resource: None }),
                    "ingestor" => Ok(DefaultPrivilege::Ingestor { resource: None }),
                    "admin" => Ok(DefaultPrivilege::Admin),
                    "writer" => Ok(DefaultPrivilege::Writer { resource: None }),
                    "editor" => Ok(DefaultPrivilege::Editor),
                    _ => Err(de::Error::unknown_variant(
                        privilege,
                        &["reader", "ingestor", "admin", "writer", "editor"],
                    )),
                })
                .collect()
        })
        .transpose()
}

fn role_has_privilege(role: &Role, privilege: &DefaultPrivilege) -> bool {
    role.privileges()
        .iter()
        .any(|candidate| std::mem::discriminant(candidate) == std::mem::discriminant(privilege))
}

impl ApiKeyListEntry {
    fn from_user(user: &User) -> Option<Self> {
        let api_key = user.as_api_key()?;
        let masked = if api_key.api_key.len() >= 4 {
            let last4 = &api_key.api_key[api_key.api_key.len() - 4..];
            format!("****{last4}")
        } else {
            "****".to_string()
        };
        Some(Self {
            key_id: api_key.key_id,
            api_key: masked,
            key_name: api_key.key_name.clone(),
            roles: user.roles.clone(),
            created_by: api_key.created_by.clone(),
            created_at: api_key.created_at,
            modified_at: api_key.modified_at,
        })
    }
}

fn tenant_or_default(tenant: &Option<String>) -> &str {
    tenant.as_deref().unwrap_or(DEFAULT_TENANT)
}

/// Collect all API-key-backed users for a tenant into an owned list.
/// Read lock on the users map is held only for the duration of this fn.
fn collect_tenant_api_keys(tenant_id: &Option<String>) -> Vec<User> {
    let users_guard = users();
    let tenant = tenant_or_default(tenant_id);
    users_guard
        .get(tenant)
        .into_iter()
        .flat_map(|m| m.values())
        .filter(|u| matches!(u.ty, UserType::ApiKey(_)))
        .cloned()
        .collect()
}

/// Validate that every role in the request exists in the tenant's role map.
/// Mirrors the check performed by the native-user CRUD endpoint.
fn validate_roles(
    role_names: &HashSet<String>,
    tenant_id: &Option<String>,
) -> Result<(), ApiKeyError> {
    let tenant = tenant_or_default(tenant_id);
    let tenant_roles = roles();
    let missing: Vec<String> = role_names
        .iter()
        .filter(|r| {
            tenant_roles
                .get(tenant)
                .map(|m| !m.contains_key(*r))
                .unwrap_or(true)
        })
        .cloned()
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(ApiKeyError::Rbac(RBACError::RolesDoNotExist(missing)))
    }
}

/// POST /api/prism/v1/apikeys
///
/// Create a new API key. Only admins can create keys.
pub async fn create_api_key(
    req: HttpRequest,
    web::Json(body): web::Json<CreateApiKeyRequest>,
) -> Result<impl Responder, ApiKeyError> {
    let (created_by, tenant_id) = verify_admin(&req)?;

    let guard = UPDATE_LOCK.lock().await;

    // Typed keys get server-managed roles; the existing UI/API flow keeps
    // assigning explicit roles when apiKeyType is omitted.
    let assigned_roles = match body.api_key_type {
        Some(api_key_type) => {
            if !body.roles.is_empty() {
                return Err(ApiKeyError::ApiKeyTypeWithRoles);
            }
            let role = match api_key_type {
                ApiKeyType::Ingestor => GLOBAL_INGESTOR_ROLE,
                ApiKeyType::Query => GLOBAL_QUERIER_ROLE,
            };
            HashSet::from([role.to_string()])
        }
        None => {
            // Hold the update lock while checking user-defined roles.
            validate_roles(&body.roles, &tenant_id)?;
            body.roles
        }
    };

    // Duplicate key-name detection inside the same tenant.
    let existing = collect_tenant_api_keys(&tenant_id);
    if existing
        .iter()
        .filter_map(|u| u.as_api_key())
        .any(|k| k.key_name == body.key_name)
    {
        return Err(ApiKeyError::DuplicateKeyName(body.key_name));
    }

    let key_id = Ulid::new();
    let api_key_value = if let Some(api_key) = body.api_key {
        validate_api_key_value(&api_key)?;
        api_key
    } else {
        generate_api_key_value()
    };
    if existing
        .iter()
        .filter_map(|user: &User| user.as_api_key())
        .any(|key| key.api_key == api_key_value)
    {
        return Err(ApiKeyError::DuplicateApiKey);
    }
    let user = User::new_api_key(
        key_id,
        api_key_value,
        body.key_name,
        assigned_roles,
        created_by,
        tenant_id.clone(),
    );

    // Persist the new user in parseable.json and push into memory on this node.
    let mut metadata = get_metadata(&tenant_id).await?;
    metadata.users.push(user.clone());
    put_metadata(&metadata, &tenant_id).await?;
    Users.put_user(user.clone());

    // Drop the lock once local state is durable; the cluster fan-out below
    // can be slow and must not block other create/delete requests.
    drop(guard);

    // Best-effort sync to other live nodes (reuses the standard user sync).
    let caller_userid = user
        .as_api_key()
        .map(|k| k.created_by.clone())
        .unwrap_or_default();
    if let Err(e) = crate::handlers::http::cluster::sync_user_creation(
        &req,
        user.clone(),
        &None,
        &tenant_id,
        &caller_userid,
    )
    .await
    {
        tracing::error!("Failed to sync API-key user creation: {e}");
    }

    let response = ApiKeyResponse::try_from(&user)?;
    Ok(HttpResponse::Ok().json(response))
}

/// DELETE /api/prism/v1/apikeys/{key_id}
///
/// Delete an API key by key_id. Only admins can delete keys.
pub async fn delete_api_key(
    req: HttpRequest,
    path: web::Path<String>,
) -> Result<impl Responder, ApiKeyError> {
    let (caller_userid, tenant_id) = verify_admin(&req)?;
    let key_id_str = path.into_inner();
    let key_id =
        Ulid::from_string(&key_id_str).map_err(|_| ApiKeyError::KeyNotFound(key_id_str.clone()))?;

    let guard = UPDATE_LOCK.lock().await;

    // Find the user entry for this api key.
    let (userid, key_name) = collect_tenant_api_keys(&tenant_id)
        .into_iter()
        .find_map(|u| {
            u.as_api_key()
                .filter(|k| k.key_id == key_id)
                .map(|k| (u.userid().to_string(), k.key_name.clone()))
        })
        .ok_or_else(|| ApiKeyError::KeyNotFound(key_id.to_string()))?;

    // Make cluster revocation part of the success contract. If this fails, we
    // leave the local key intact so the caller can retry the same revocation.
    if let Err(e) = crate::handlers::http::cluster::sync_user_deletion_with_ingestors(
        &req,
        &userid,
        &tenant_id,
        &caller_userid,
    )
    .await
    {
        tracing::error!("Failed to sync API-key user deletion: {e}");
        return Err(ApiKeyError::RevocationSyncFailed(e.to_string()));
    }

    // Remove from parseable.json.
    let mut metadata = get_metadata(&tenant_id).await?;
    metadata.users.retain(|u| u.userid() != userid.as_str());
    put_metadata(&metadata, &tenant_id).await?;

    // Remove from in-memory map on this node.
    Users.delete_user(&userid, &tenant_id);

    drop(guard);

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "keyId": key_id,
        "keyName": key_name,
        "message": "API key deleted successfully",
    })))
}

/// GET /api/prism/v1/apikeys
///
/// List all API keys (masked). Only admins can list keys. Optionally filter by
/// one or more comma-separated role privileges, such as
/// `?priviledge=reader,editor,admin`.
pub async fn list_api_keys(
    req: HttpRequest,
    web::Query(query): web::Query<ListApiKeysQuery>,
) -> Result<impl Responder, ApiKeyError> {
    let (_, tenant_id) = verify_admin(&req)?;
    let tenant = tenant_or_default(&tenant_id);
    let tenant_roles = roles();
    let entries: Vec<ApiKeyListEntry> = collect_tenant_api_keys(&tenant_id)
        .iter()
        .filter(|user| {
            query.priviledge.as_ref().is_none_or(|privileges| {
                user.roles.iter().any(|role_name| {
                    tenant_roles
                        .get(tenant)
                        .and_then(|roles| roles.get(role_name))
                        .is_some_and(|role| {
                            privileges
                                .iter()
                                .any(|privilege| role_has_privilege(role, privilege))
                        })
                })
            })
        })
        .filter_map(ApiKeyListEntry::from_user)
        .collect();
    Ok(HttpResponse::Ok().json(entries))
}

/// POST /api/prism/v1/apikeys/validate
///
/// Validate whether an API key exists in the caller's tenant.
pub async fn validate_api_key(
    req: HttpRequest,
    web::Json(body): web::Json<ValidateApiKeyRequest>,
) -> Result<impl Responder, ApiKeyError> {
    let (_, tenant_id) = get_user_and_tenant_from_request(&req)
        .map_err(|_| ApiKeyError::Unauthorized("Missing user identity".into()))?;
    let valid = collect_tenant_api_keys(&tenant_id)
        .iter()
        .filter_map(|u| u.as_api_key())
        .any(|k| k.api_key == body.api_key);

    Ok(HttpResponse::Ok().json(ValidateApiKeyResponse { valid }))
}

/// GET /api/prism/v1/apikeys/{key_id}
///
/// Get a specific API key (full key visible). Only admins can get keys.
pub async fn get_api_key(
    req: HttpRequest,
    path: web::Path<String>,
) -> Result<impl Responder, ApiKeyError> {
    let (_, tenant_id) = verify_admin(&req)?;
    let key_id_str = path.into_inner();
    let key_id =
        Ulid::from_string(&key_id_str).map_err(|_| ApiKeyError::KeyNotFound(key_id_str.clone()))?;

    let user = collect_tenant_api_keys(&tenant_id)
        .into_iter()
        .find(|u| u.as_api_key().map(|k| k.key_id == key_id).unwrap_or(false))
        .ok_or_else(|| ApiKeyError::KeyNotFound(key_id.to_string()))?;

    let response = ApiKeyResponse::try_from(&user)?;
    Ok(HttpResponse::Ok().json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_single_api_key_privilege() {
        let query: ListApiKeysQuery =
            serde_json::from_value(serde_json::json!({ "priviledge": "reader" })).unwrap();
        let privileges = query.priviledge.unwrap();

        assert_eq!(privileges.len(), 1);
        assert!(matches!(
            privileges.first(),
            Some(DefaultPrivilege::Reader { .. })
        ));
    }

    #[test]
    fn deserializes_comma_separated_api_key_privileges() {
        let query =
            web::Query::<ListApiKeysQuery>::from_query("priviledge=reader%2Ceditor%2Cadmin")
                .unwrap()
                .into_inner();
        let privileges = query.priviledge.unwrap();

        assert_eq!(privileges.len(), 3);
        assert!(matches!(
            privileges.first(),
            Some(DefaultPrivilege::Reader { .. })
        ));
        assert!(matches!(privileges.get(1), Some(DefaultPrivilege::Editor)));
        assert!(matches!(privileges.get(2), Some(DefaultPrivilege::Admin)));
    }

    #[test]
    fn allows_spaces_around_api_key_privileges() {
        let query: ListApiKeysQuery = serde_json::from_value(serde_json::json!({
            "priviledge": "reader, writer"
        }))
        .unwrap();
        let privileges = query.priviledge.unwrap();

        assert_eq!(privileges.len(), 2);
        assert!(matches!(
            privileges.get(1),
            Some(DefaultPrivilege::Writer { .. })
        ));
    }

    #[test]
    fn rejects_unknown_api_key_privilege() {
        let query = serde_json::from_value::<ListApiKeysQuery>(serde_json::json!({
            "priviledge": "reader,owner"
        }));

        assert!(query.is_err());
    }
}
