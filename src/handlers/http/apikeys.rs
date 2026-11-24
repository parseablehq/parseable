use std::collections::HashSet;

use actix_web::{HttpRequest, HttpResponse, Responder, web};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, de};
use ulid::Ulid;

use crate::{
    apikeys::{ApiKeyError, CreateApiKeyRequest},
    handlers::http::{
        modal::utils::rbac_utils::{get_metadata, put_metadata},
        rbac::{RBACError, UPDATE_LOCK},
    },
    parseable::DEFAULT_TENANT,
    rbac::{
        Users,
        map::{read_user_groups, roles, users},
        role::model::{DefaultPrivilege, Role, RoleType},
        user::{User, UserType, hash_api_key},
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

/// Serialisable shape returned by create and get. `api_key` is the key itself
/// only in the response to create, the one time the server has it; get returns it
/// masked, as list does.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiKeyResponse<'a> {
    key_id: Ulid,
    api_key: String,
    key_name: &'a str,
    roles: &'a HashSet<String>,
    created_by: &'a str,
    created_at: DateTime<Utc>,
    modified_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
}

impl<'a> ApiKeyResponse<'a> {
    /// The response to creating `user` with the key `created_key`, which is
    /// returned this once
    fn created(user: &'a User, created_key: String) -> Result<Self, ApiKeyError> {
        Self::new(user, Some(created_key))
    }

    /// The details of `user`, with its key masked
    fn masked(user: &'a User) -> Result<Self, ApiKeyError> {
        Self::new(user, None)
    }

    fn new(user: &'a User, created_key: Option<String>) -> Result<Self, ApiKeyError> {
        let api_key = user
            .as_api_key()
            .ok_or_else(|| ApiKeyError::KeyNotFound(user.userid().to_string()))?;
        Ok(Self {
            key_id: api_key.key_id,
            api_key: created_key.unwrap_or_else(|| api_key.masked_key()),
            key_name: &api_key.key_name,
            roles: &user.roles,
            created_by: &api_key.created_by,
            created_at: api_key.created_at,
            modified_at: api_key.modified_at,
            expires_at: api_key.expires_at,
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
    expires_at: Option<DateTime<Utc>>,
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

/// Query params for `GET /apikeys`. `privilege` takes one of `reader`,
/// `ingestor`, `admin`, `writer`, `editor` or `superadmin`, matched by kind.
/// `privilege` is the canonical name; `priviledge` is accepted as an alias for
/// compatibility with upstream Parseable clients, which use that spelling.
/// Unknown parameter names, such as `Privilege`, are ignored and leave the list
/// unfiltered.
#[derive(Debug, Deserialize)]
pub struct ListApiKeysQuery {
    #[serde(
        default,
        alias = "priviledge",
        deserialize_with = "deserialize_api_key_privilege"
    )]
    privilege: Option<DefaultPrivilege>,
}

fn deserialize_api_key_privilege<'de, D>(
    deserializer: D,
) -> Result<Option<DefaultPrivilege>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|privilege| match privilege.as_str() {
            "reader" => Ok(DefaultPrivilege::Reader { resource: None }),
            "ingestor" => Ok(DefaultPrivilege::Ingestor { resource: None }),
            "admin" => Ok(DefaultPrivilege::Admin),
            "writer" => Ok(DefaultPrivilege::Writer { resource: None }),
            "editor" => Ok(DefaultPrivilege::Editor),
            "superadmin" => Ok(DefaultPrivilege::SuperAdmin),
            _ => Err(de::Error::unknown_variant(
                &privilege,
                &[
                    "reader",
                    "ingestor",
                    "admin",
                    "writer",
                    "editor",
                    "superadmin",
                ],
            )),
        })
        .transpose()
}

fn role_has_privilege(role: &Role, privilege: &DefaultPrivilege) -> bool {
    role.privileges()
        .iter()
        .any(|candidate| std::mem::discriminant(candidate) == std::mem::discriminant(privilege))
}

/// Masked list entries for the tenant's API keys whose direct or group-inherited
/// roles grant `privilege`, or all of them when `privilege` is `None`.
/// Groups and roles missing from the tenant maps never match.
fn tenant_entries(
    tenant_id: &Option<String>,
    privilege: Option<&DefaultPrivilege>,
) -> Vec<ApiKeyListEntry> {
    let keys = collect_tenant_api_keys(tenant_id);
    let matching: Vec<&User> = match privilege {
        None => keys.iter().collect(),
        Some(privilege) => {
            let tenant = tenant_or_default(tenant_id);
            // Lock order: user groups before roles. Only the granting role and
            // group names are computed under the guards; both drop before the
            // per-key scan so it never blocks writers or the readers behind them.
            let (granting_roles, granting_groups) = {
                let all_groups = read_user_groups();
                let all_roles = roles();
                let granting_roles: HashSet<String> = all_roles
                    .get(tenant)
                    .into_iter()
                    .flatten()
                    .filter(|(_, role)| role_has_privilege(role, privilege))
                    .map(|(name, _)| name.clone())
                    .collect();
                let granting_groups: HashSet<String> = all_groups
                    .get(tenant)
                    .into_iter()
                    .flatten()
                    .filter(|(_, group)| !group.roles.is_disjoint(&granting_roles))
                    .map(|(name, _)| name.clone())
                    .collect();
                (granting_roles, granting_groups)
            };
            keys.iter()
                .filter(|user| {
                    !user.roles.is_disjoint(&granting_roles)
                        || !user.user_groups.is_disjoint(&granting_groups)
                })
                .collect()
        }
    };
    matching
        .into_iter()
        .filter_map(ApiKeyListEntry::from_user)
        .collect()
}

impl ApiKeyListEntry {
    fn from_user(user: &User) -> Option<Self> {
        let api_key = user.as_api_key()?;
        Some(Self {
            key_id: api_key.key_id,
            api_key: api_key.masked_key(),
            key_name: api_key.key_name.clone(),
            roles: user.roles.clone(),
            created_by: api_key.created_by.clone(),
            created_at: api_key.created_at,
            modified_at: api_key.modified_at,
            expires_at: api_key.expires_at,
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

/// Validate that every requested role exists and can be assigned in this tenant.
/// Mirrors the check performed by the native-user CRUD endpoint.
fn validate_roles(
    role_names: &HashSet<String>,
    tenant_id: &Option<String>,
) -> Result<(), ApiKeyError> {
    let tenant = tenant_or_default(tenant_id);
    let missing: Vec<String> = {
        let tenant_roles = roles();
        role_names
            .iter()
            .filter(|r| {
                tenant_roles
                    .get(tenant)
                    .map(|m| !m.contains_key(*r))
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    };
    if !missing.is_empty() {
        return Err(ApiKeyError::Rbac(RBACError::RolesDoNotExist(missing)));
    }
    // Takes USERS, so no ROLES guard may be held here.
    if Users.any_role_held_by_protected_user(role_names, tenant_id) {
        return Err(ApiKeyError::Rbac(RBACError::ProtectedRole));
    }
    if let Some(tenant_roles) = roles().get(tenant)
        && role_names.iter().any(|name| {
            tenant_roles.get(name).is_some_and(|role| {
                role.role_type() == &RoleType::Internal || role.deny_super_admin()
            })
        })
    {
        return Err(ApiKeyError::Rbac(RBACError::ProtectedRole));
    }
    Ok(())
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

    // Validate role assignments under the update lock so a concurrent role
    // update cannot invalidate the check.
    validate_roles(&body.roles, &tenant_id)?;

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
    // The key is kept only until the response is written: the user holds its hash
    let api_key_value = generate_api_key_value();
    let mut user = User::new_api_key(
        key_id,
        api_key_value.clone(),
        body.key_name,
        body.roles,
        created_by,
        tenant_id.clone(),
    );

    if let UserType::ApiKey(key) = &mut user.ty {
        key.expires_at = body
            .ttl_seconds
            .map(|seconds| key.created_at + chrono::TimeDelta::seconds(seconds));
    }

    // Persist the new user in parseable.json and push into memory on this node.
    let mut metadata = get_metadata(&tenant_id).await?;
    metadata.users.push(user.clone());
    put_metadata(&metadata, &tenant_id).await?;
    Users.put_user(user.clone());

    // Drop the lock once local state is durable; the cluster fan-out below
    // can be slow and must not block other create/delete requests.
    drop(guard);

    // Node delivery is best effort; invalid forwarding headers still fail the request.
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
        if matches!(&e, RBACError::HeaderBuild(_)) {
            return Err(e.into());
        }
        tracing::error!("Failed to sync API-key user creation: {e}");
    }

    let response = ApiKeyResponse::created(&user, api_key_value)?;
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

    // Remove from parseable.json first: ingestors install what storage holds,
    // so a sync sent before this would reinstall the key.
    let mut metadata = get_metadata(&tenant_id).await?;
    metadata.users.retain(|u| u.userid() != userid.as_str());
    put_metadata(&metadata, &tenant_id).await?;

    // Make cluster revocation part of the success contract. If this fails, we
    // leave the local key intact so the caller can retry the same revocation;
    // storage no longer has it, so a restart drops it too.
    if let Err(e) = crate::handlers::http::cluster::sync_user_deletion_with_ingestors(
        &req,
        &userid,
        &tenant_id,
        &caller_userid,
    )
    .await
    {
        if matches!(&e, RBACError::HeaderBuild(_)) {
            return Err(e.into());
        }
        tracing::error!("Failed to sync API-key user deletion: {e}");
        return Err(ApiKeyError::RevocationSyncFailed(e.to_string()));
    }

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
/// role privilege with
/// `?privilege=reader|ingestor|admin|writer|editor|superadmin` (`?priviledge=`
/// is accepted too). Privileges match by kind, so `admin` does not match
/// super-admin keys. An unknown value is rejected with 400.
/// Unknown parameter names are ignored and return the unfiltered list.
pub async fn list_api_keys(
    req: HttpRequest,
    web::Query(query): web::Query<ListApiKeysQuery>,
) -> Result<impl Responder, ApiKeyError> {
    let (_, tenant_id) = verify_admin(&req)?;
    let entries = tenant_entries(&tenant_id, query.privilege.as_ref());
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
    let now = Utc::now();
    let hash = hash_api_key(&body.api_key);
    let valid = collect_tenant_api_keys(&tenant_id)
        .iter()
        .filter_map(|u| u.as_api_key())
        .any(|k| k.matches_active_hash(&hash, now));

    Ok(HttpResponse::Ok().json(ValidateApiKeyResponse { valid }))
}

/// GET /api/prism/v1/apikeys/{key_id}
///
/// Get a specific API key. Only admins can get keys. The key itself is not
/// kept, so `apiKey` is masked here as it is in the list: the key is returned
/// once, by the request that creates it.
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

    let response = ApiKeyResponse::masked(&user)?;
    Ok(HttpResponse::Ok().json(response))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use actix_web::{App, cookie::Cookie, http::StatusCode, test as actix_test};
    use parking_lot::RwLock;
    use tokio::sync::MutexGuard;

    use super::*;
    use crate::handlers::http::modal::server::Server;
    use crate::rbac::{
        map::{
            self, ROLES, SESSIONS, SessionKey, TEST_SERIAL, USER_GROUPS, USERS, UserGroups,
            mut_roles, mut_users, write_user_groups,
        },
        role::ParseableResourceType,
        user::UserGroup,
    };

    #[test]
    fn expired_key_remains_visible_with_expiry_in_detail_and_list() {
        let mut user = User::new_api_key(
            Ulid::new(),
            "test-secret".into(),
            "test".into(),
            HashSet::new(),
            "admin".into(),
            None,
        );
        let expiry = DateTime::parse_from_rfc3339("2026-01-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let UserType::ApiKey(key) = &mut user.ty else {
            unreachable!()
        };
        key.expires_at = Some(expiry);
        assert!(!key.matches_active_key("test-secret", expiry));
        let detail = serde_json::to_value(ApiKeyResponse::masked(&user).unwrap()).unwrap();
        let entry = serde_json::to_value(ApiKeyListEntry::from_user(&user).unwrap()).unwrap();
        assert_eq!(detail["expiresAt"], serde_json::to_value(expiry).unwrap());
        assert_eq!(entry["expiresAt"], detail["expiresAt"]);
        assert_eq!(entry["apiKey"], "****cret");
        assert_eq!(detail["apiKey"], "****cret");
    }

    /// The key is in the response to create and nowhere after it: get and list
    /// return the mask, and neither they nor the stored user hold the key or
    /// its hash where a response could show it.
    #[test]
    fn key_is_returned_by_create_only() {
        let key = generate_api_key_value();
        let user = User::new_api_key(
            Ulid::new(),
            key.clone(),
            "test".into(),
            HashSet::new(),
            "admin".into(),
            None,
        );
        let created =
            serde_json::to_value(ApiKeyResponse::created(&user, key.clone()).unwrap()).unwrap();
        assert_eq!(created["apiKey"], key.as_str());

        let masked = format!("****{}", &key[key.len() - 4..]);
        let detail = serde_json::to_string(&ApiKeyResponse::masked(&user).unwrap()).unwrap();
        let entry = serde_json::to_string(&ApiKeyListEntry::from_user(&user).unwrap()).unwrap();
        let hash = hash_api_key(&key);
        for response in [&detail, &entry] {
            let value: serde_json::Value = serde_json::from_str(response).unwrap();
            assert_eq!(value["apiKey"], masked.as_str());
            assert!(!response.contains(&key), "{response}");
            assert!(!response.contains(&hash), "{response}");
            assert!(value.get("keyHash").is_none(), "{response}");
        }
        // The same fields as the response to create, so clients read both alike
        let detail: serde_json::Value = serde_json::from_str(&detail).unwrap();
        assert_eq!(
            detail.as_object().unwrap().keys().collect::<Vec<_>>(),
            created.as_object().unwrap().keys().collect::<Vec<_>>()
        );
        assert!(!serde_json::to_string(&user).unwrap().contains(&key));
    }

    #[test]
    fn non_expiring_responses_include_null_expiry() {
        let user = User::new_api_key(
            Ulid::new(),
            "test-secret".into(),
            "test".into(),
            HashSet::new(),
            "admin".into(),
            None,
        );
        let detail = serde_json::to_value(ApiKeyResponse::masked(&user).unwrap()).unwrap();
        let entry = serde_json::to_value(ApiKeyListEntry::from_user(&user).unwrap()).unwrap();
        assert!(detail.get("expiresAt").unwrap().is_null());
        assert!(entry.get("expiresAt").unwrap().is_null());
    }

    fn parse_query(query: &str) -> Result<ListApiKeysQuery, actix_web::Error> {
        web::Query::<ListApiKeysQuery>::from_query(query)
            .map(web::Query::into_inner)
            .map_err(Into::into)
    }

    #[test]
    fn list_query_without_privilege_is_unfiltered() {
        for query in ["", "Privilege=reader", "privilage=reader", "_=123"] {
            assert!(parse_query(query).unwrap().privilege.is_none(), "{query}");
        }
    }

    #[test]
    fn list_query_accepts_every_privilege_under_both_spellings() {
        let cases = [
            ("reader", DefaultPrivilege::Reader { resource: None }),
            ("ingestor", DefaultPrivilege::Ingestor { resource: None }),
            ("admin", DefaultPrivilege::Admin),
            ("writer", DefaultPrivilege::Writer { resource: None }),
            ("editor", DefaultPrivilege::Editor),
        ];
        for (value, expected) in cases {
            for param in ["privilege", "priviledge"] {
                let parsed = parse_query(&format!("{param}={value}"))
                    .unwrap_or_else(|err| panic!("{param}={value}: {err}"))
                    .privilege;
                assert_eq!(parsed, Some(expected.clone()), "{param}={value}");
            }
        }
    }

    #[test]
    fn list_query_accepts_superadmin_privilege() {
        let parsed = parse_query("privilege=superadmin").unwrap().privilege;
        assert_eq!(parsed, Some(DefaultPrivilege::SuperAdmin));
    }

    #[test]
    fn list_query_accepts_superadmin_priviledge_alias() {
        let parsed = parse_query("priviledge=superadmin").unwrap().privilege;
        assert_eq!(parsed, Some(DefaultPrivilege::SuperAdmin));
    }

    #[test]
    fn list_query_rejects_unknown_privilege_with_bad_request() {
        let cases = [
            ("privilege=owner", "unknown variant `owner`"),
            ("priviledge=owner", "unknown variant `owner`"),
            ("privilege=Reader", "unknown variant `Reader`"),
            ("privilege=", "unknown variant ``"),
            ("privilege=reader&priviledge=reader", "duplicate field"),
            ("privilege=reader&privilege=admin", "duplicate field"),
        ];
        for (query, message) in cases {
            let err = parse_query(query).expect_err(query);
            assert_eq!(
                err.as_response_error().status_code(),
                actix_web::http::StatusCode::BAD_REQUEST,
                "{query}"
            );
            assert!(err.to_string().contains(message), "{query}: {err}");
        }
    }

    // `map::init` needs `PARSEABLE`, so tests create the maps directly. Tests
    // that write them hold `map::TEST_SERIAL`, as other modules' tests watch the
    // map locks. Each test uses its own tenant, so they don't see each other's
    // keys or roles.
    async fn rbac_state() -> MutexGuard<'static, ()> {
        let serial = TEST_SERIAL.lock().await;
        init_rbac_maps();
        serial
    }

    fn blocking_rbac_state() -> MutexGuard<'static, ()> {
        let serial = TEST_SERIAL.blocking_lock();
        init_rbac_maps();
        serial
    }

    fn init_rbac_maps() {
        USERS.get_or_init(|| RwLock::new(map::Users::default()));
        SESSIONS.get_or_init(|| RwLock::new(map::Sessions::default()));
        USER_GROUPS.get_or_init(|| RwLock::new(UserGroups::default()));
        ROLES.get_or_init(|| RwLock::new(HashMap::new()));
    }

    fn tenant(roles: Vec<(&str, Vec<DefaultPrivilege>)>, groups: &[(&str, &str)]) -> String {
        let tenant = format!("apikeys-{}", Ulid::new());
        mut_roles().insert(
            tenant.clone(),
            roles
                .into_iter()
                .map(|(name, privileges)| (name.to_owned(), Role::create_user_role(privileges)))
                .collect(),
        );
        for (name, role) in groups {
            let group = UserGroup {
                name: (*name).to_owned(),
                roles: HashSet::from([(*role).to_owned()]),
                ..Default::default()
            };
            write_user_groups().insert(group, &tenant);
        }
        tenant
    }

    fn add_key(tenant: &str, name: &str, roles: &[&str], groups: &[&str]) {
        let mut key = User::new_api_key(
            Ulid::new(),
            format!("{name}-secret"),
            name.into(),
            roles.iter().map(|r| r.to_string()).collect(),
            "admin".into(),
            Some(tenant.to_owned()),
        );
        key.user_groups = groups.iter().map(|g| g.to_string()).collect();
        mut_users().insert(key);
    }

    fn names(tenant: &str, privilege: Option<DefaultPrivilege>) -> Vec<String> {
        let mut names: Vec<String> = tenant_entries(&Some(tenant.to_owned()), privilege.as_ref())
            .into_iter()
            .map(|entry| entry.key_name)
            .collect();
        names.sort();
        names
    }

    #[test]
    fn list_without_privilege_returns_every_key_masked() {
        let _serial = blocking_rbac_state();
        let tenant = tenant(
            vec![("readers", vec![DefaultPrivilege::Reader { resource: None }])],
            &[],
        );
        add_key(&tenant, "reader-key", &["readers"], &[]);
        add_key(&tenant, "dangling-key", &["deleted-role"], &[]);
        add_key(&tenant, "roleless-key", &[], &[]);
        let entries = tenant_entries(&Some(tenant.clone()), None);
        assert!(entries.iter().all(|entry| entry.api_key == "****cret"));
        assert_eq!(
            names(&tenant, None),
            ["dangling-key", "reader-key", "roleless-key"]
        );
    }

    #[test]
    fn privilege_filter_matches_keys_by_role_privileges() {
        let _serial = blocking_rbac_state();
        let tenant = tenant(
            vec![
                (
                    "readers",
                    vec![DefaultPrivilege::Reader {
                        resource: Some(ParseableResourceType::Stream("logs".into())),
                    }],
                ),
                (
                    "ingest-and-read",
                    vec![
                        DefaultPrivilege::Ingestor { resource: None },
                        DefaultPrivilege::Reader { resource: None },
                    ],
                ),
                ("admins", vec![DefaultPrivilege::Admin]),
                ("writers", vec![DefaultPrivilege::Writer { resource: None }]),
                ("editors", vec![DefaultPrivilege::Editor]),
            ],
            &[],
        );
        add_key(&tenant, "reader-key", &["readers"], &[]);
        add_key(&tenant, "pipeline-key", &["ingest-and-read"], &[]);
        add_key(&tenant, "admin-key", &["admins"], &[]);
        add_key(&tenant, "writer-key", &["writers"], &[]);
        add_key(&tenant, "editor-key", &["editors"], &[]);
        add_key(&tenant, "dangling-key", &["deleted-role"], &[]);
        add_key(&tenant, "roleless-key", &[], &[]);

        // Resource-scoped privileges still match the bare privilege filter.
        assert_eq!(
            names(&tenant, Some(DefaultPrivilege::Reader { resource: None })),
            ["pipeline-key", "reader-key"]
        );
        assert_eq!(
            names(&tenant, Some(DefaultPrivilege::Ingestor { resource: None })),
            ["pipeline-key"]
        );
        // Privileges match by kind, not hierarchy: admin keys are not readers.
        assert_eq!(names(&tenant, Some(DefaultPrivilege::Admin)), ["admin-key"]);
        assert_eq!(
            names(&tenant, Some(DefaultPrivilege::Writer { resource: None })),
            ["writer-key"]
        );
        assert_eq!(
            names(&tenant, Some(DefaultPrivilege::Editor)),
            ["editor-key"]
        );
    }

    #[test]
    fn privilege_filter_matches_superadmin_direct_and_group_roles_exactly() {
        let _serial = blocking_rbac_state();
        let tenant = tenant(
            vec![
                ("superadmins", vec![DefaultPrivilege::SuperAdmin]),
                ("admins", vec![DefaultPrivilege::Admin]),
            ],
            &[("superadmins-group", "superadmins")],
        );
        add_key(&tenant, "superadmin-key", &["superadmins"], &[]);
        add_key(&tenant, "superadmin-group-key", &[], &["superadmins-group"]);
        add_key(&tenant, "admin-key", &["admins"], &[]);
        assert_eq!(
            names(&tenant, Some(DefaultPrivilege::SuperAdmin)),
            ["superadmin-group-key", "superadmin-key"]
        );
        assert_eq!(names(&tenant, Some(DefaultPrivilege::Admin)), ["admin-key"]);
    }

    #[actix_web::test]
    async fn list_route_filters_privilege_and_requires_admin() {
        let _serial = rbac_state().await;
        let tenant = tenant(
            vec![
                ("admins", vec![DefaultPrivilege::Admin]),
                ("readers", vec![DefaultPrivilege::Reader { resource: None }]),
            ],
            &[],
        );
        add_key(&tenant, "admin-key", &["admins"], &[]);
        add_key(&tenant, "reader-key", &["readers"], &[]);
        let mut cookies = Vec::new();
        for role in ["admins", "readers"] {
            let (mut user, _) = User::new_basic(
                format!("apikeys-{}", Ulid::new()).to_lowercase(),
                Some(tenant.clone()),
                false,
            );
            user.roles.insert(role.into());
            mut_users().insert(user.clone());
            let session = Ulid::new();
            Users.new_session(
                &user,
                SessionKey::SessionId(session),
                chrono::TimeDelta::hours(1),
            );
            cookies.push(Cookie::new("session", session.to_string()));
        }
        let app = actix_test::init_service(
            App::new().service(web::scope("/api/prism/v1").service(Server::get_apikeys_webscope())),
        )
        .await;
        for (query, expected) in [
            ("", vec!["admin-key", "reader-key"]),
            ("?privilege=admin", vec!["admin-key"]),
        ] {
            let req = actix_test::TestRequest::get()
                .uri(&format!("/api/prism/v1/apikeys{query}"))
                .cookie(cookies[0].clone())
                .to_request();
            let response = actix_test::call_service(&app, req).await;
            assert_eq!(response.status(), StatusCode::OK, "{query}");
            let body: serde_json::Value = actix_test::read_body_json(response).await;
            let mut names: Vec<_> = body
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| entry["keyName"].as_str().unwrap())
                .collect();
            names.sort();
            assert_eq!(names, expected, "{query}");
        }
        for (query, cookie, expected) in [
            (
                "?privilege=bogus",
                Some(&cookies[0]),
                StatusCode::BAD_REQUEST,
            ),
            ("", None, StatusCode::UNAUTHORIZED),
            ("", Some(&cookies[1]), StatusCode::FORBIDDEN),
        ] {
            let mut req =
                actix_test::TestRequest::get().uri(&format!("/api/prism/v1/apikeys{query}"));
            if let Some(cookie) = cookie {
                req = req.cookie(cookie.clone());
            }
            let status = match actix_test::try_call_service(&app, req.to_request()).await {
                Ok(response) => response.status(),
                Err(error) => error.as_response_error().status_code(),
            };
            assert_eq!(status, expected, "{query}");
        }
    }

    #[test]
    fn privilege_filter_includes_group_inherited_roles() {
        let _serial = blocking_rbac_state();
        let tenant = tenant(
            vec![("readers", vec![DefaultPrivilege::Reader { resource: None }])],
            &[
                ("readers-group", "readers"),
                ("dangling-group", "deleted-role"),
            ],
        );
        add_key(&tenant, "group-key", &[], &["readers-group"]);
        add_key(&tenant, "dangling-group-key", &[], &["dangling-group"]);
        add_key(&tenant, "missing-group-key", &[], &["missing-group"]);
        assert_eq!(
            names(&tenant, Some(DefaultPrivilege::Reader { resource: None })),
            ["group-key"]
        );
        assert!(names(&tenant, Some(DefaultPrivilege::Admin)).is_empty());
    }

    #[test]
    fn privilege_filter_resolves_roles_in_the_given_tenant() {
        let _serial = blocking_rbac_state();
        // Same role and group names, different privileges per tenant.
        let readers = tenant(
            vec![("team", vec![DefaultPrivilege::Reader { resource: None }])],
            &[("team-group", "team")],
        );
        let admins = tenant(
            vec![("team", vec![DefaultPrivilege::Admin])],
            &[("team-group", "team")],
        );
        add_key(&readers, "reader-key", &["team"], &[]);
        add_key(&readers, "reader-group-key", &[], &["team-group"]);
        add_key(&admins, "admin-key", &["team"], &[]);
        add_key(&admins, "admin-group-key", &[], &["team-group"]);

        let reader = || Some(DefaultPrivilege::Reader { resource: None });
        assert_eq!(
            names(&readers, reader()),
            ["reader-group-key", "reader-key"]
        );
        assert!(names(&readers, Some(DefaultPrivilege::Admin)).is_empty());
        assert_eq!(
            names(&admins, Some(DefaultPrivilege::Admin)),
            ["admin-group-key", "admin-key"]
        );
        assert!(names(&admins, reader()).is_empty());

        // Keys in a tenant without a role map match no filter.
        let unknown = format!("apikeys-{}", Ulid::new());
        add_key(&unknown, "orphan-key", &["team"], &[]);
        assert_eq!(names(&unknown, None), ["orphan-key"]);
        assert!(names(&unknown, reader()).is_empty());
    }

    mod role_escalation {
        use actix_web::{HttpMessage, ResponseError, test::TestRequest};
        use chrono::TimeDelta;

        use super::*;
        use crate::rbac::user::Basic;

        const ADMIN_ROLE: &str = "tenant-admin";
        const READER_ROLE: &str = "reader";
        const INTERNAL_ROLE: &str = "internal";
        // Mirrors the internal role `rbac::map::init` adds to the default tenant.
        const SUPER_ADMIN_ROLE: &str = "super-admin";
        // A user-typed role holding SuperAdmin, e.g. from legacy metadata.
        const ELEVATED_ROLE: &str = "elevated";

        /// A fresh tenant laid out like the default tenant after `map::init`.
        fn tenant() -> String {
            let tenant = format!("apikey-escalation-{}", Ulid::new());
            mut_roles().insert(
                tenant.clone(),
                HashMap::from([
                    (
                        ADMIN_ROLE.to_owned(),
                        Role::create_user_role(vec![DefaultPrivilege::Admin]),
                    ),
                    (
                        READER_ROLE.to_owned(),
                        Role::create_user_role(vec![DefaultPrivilege::Reader { resource: None }]),
                    ),
                    (
                        INTERNAL_ROLE.to_owned(),
                        Role::create_internal_role(vec![DefaultPrivilege::Admin]),
                    ),
                    (
                        SUPER_ADMIN_ROLE.to_owned(),
                        Role::create_internal_role(vec![DefaultPrivilege::SuperAdmin]),
                    ),
                    (
                        ELEVATED_ROLE.to_owned(),
                        Role::create_user_role(vec![DefaultPrivilege::SuperAdmin]),
                    ),
                ]),
            );
            tenant
        }

        fn role_set(name: &str) -> HashSet<String> {
            HashSet::from([name.to_owned()])
        }

        /// A request from a tenant admin (Admin, not SuperAdmin) with a live session.
        fn tenant_admin_request(tenant: &str) -> HttpRequest {
            let admin = User {
                ty: UserType::Native(Basic {
                    username: format!("admin-{}", Ulid::new()),
                    password_hash: String::new(),
                    email: None,
                }),
                roles: role_set(ADMIN_ROLE),
                user_groups: HashSet::new(),
                tenant: Some(tenant.to_owned()),
                protected: false,
            };
            mut_users().insert(admin.clone());
            let session = SessionKey::SessionId(Ulid::new());
            Users.new_session(&admin, session.clone(), TimeDelta::minutes(5));
            let req = TestRequest::default().to_http_request();
            req.extensions_mut().insert(session);
            req
        }

        #[test]
        fn validate_roles_accepts_ordinary_role() {
            let _serial = blocking_rbac_state();
            let tenant = Some(tenant());
            assert!(validate_roles(&role_set(READER_ROLE), &tenant).is_ok());
        }

        #[test]
        fn validate_roles_rejects_internal_role() {
            let _serial = blocking_rbac_state();
            let tenant = Some(tenant());
            let result = validate_roles(&role_set(INTERNAL_ROLE), &tenant);
            assert!(
                matches!(result, Err(ApiKeyError::Rbac(RBACError::ProtectedRole))),
                "internal role must be rejected like the native-user path, got {result:?}"
            );
        }

        #[test]
        fn validate_roles_rejects_super_admin_privilege_role() {
            let _serial = blocking_rbac_state();
            let tenant = Some(tenant());
            let result = validate_roles(&role_set(ELEVATED_ROLE), &tenant);
            assert!(
                matches!(result, Err(ApiKeyError::Rbac(RBACError::ProtectedRole))),
                "role holding SuperAdmin must be rejected like the native-user path, got {result:?}"
            );
        }

        #[test]
        fn validate_roles_rejects_role_held_by_protected_user() {
            let _serial = blocking_rbac_state();
            let tenant = tenant();
            mut_users().insert(User {
                ty: UserType::Native(Basic {
                    username: format!("protected-{}", Ulid::new()),
                    password_hash: String::new(),
                    email: None,
                }),
                roles: role_set(READER_ROLE),
                user_groups: HashSet::new(),
                tenant: Some(tenant.clone()),
                protected: true,
            });
            let result = validate_roles(&role_set(READER_ROLE), &Some(tenant));
            assert!(
                matches!(result, Err(ApiKeyError::Rbac(RBACError::ProtectedRole))),
                "a protected user's role must be rejected like the native-user path, got {result:?}"
            );
        }

        #[tokio::test]
        async fn tenant_admin_cannot_create_super_admin_api_key() {
            let _serial = rbac_state().await;
            let tenant = tenant();
            let req = tenant_admin_request(&tenant);
            // An existing key with the same name makes the vulnerable handler stop
            // with 409 before it reaches storage, which tests cannot initialise.
            let key_name = "escalation".to_owned();
            mut_users().insert(User::new_api_key(
                Ulid::new(),
                format!("existing-{}", Ulid::new()),
                key_name.clone(),
                HashSet::new(),
                "admin".into(),
                Some(tenant.clone()),
            ));

            for role in [SUPER_ADMIN_ROLE, ELEVATED_ROLE] {
                let body = CreateApiKeyRequest {
                    key_name: key_name.clone(),
                    roles: role_set(role),
                    ttl_seconds: None,
                };
                let Err(err) = create_api_key(req.clone(), web::Json(body)).await else {
                    panic!("API key with role {role} must be rejected");
                };
                assert!(
                    matches!(err, ApiKeyError::Rbac(RBACError::ProtectedRole)),
                    "role {role}: expected ProtectedRole, got {err:?}"
                );
                assert_eq!(err.status_code(), StatusCode::BAD_REQUEST, "role {role}");
            }
        }
    }
}
