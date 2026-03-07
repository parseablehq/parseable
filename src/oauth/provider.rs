use std::{
    any::Any,
    collections::{HashMap, HashSet},
};

use actix_web::http::header::HeaderMap;
use async_trait::async_trait;
use openid::Bearer;
use url::Url;

use crate::rbac::user::OAuth;

/// Trait implemented by every OAuth provider.
///
#[async_trait]
pub trait OAuthProvider: Send + Sync + Any {
    /// Build the redirect-to-provider authorization URL.
    fn auth_url(&self, scope: &str, state: Option<String>) -> Url;

    /// Exchange an authorization code for the complete session data.
    ///
    /// Implementors are responsible for all internal steps:
    /// - token request
    /// - ID-token decoding and validation
    /// - JWKS key rotation / reconnection (OIDC-specific)
    /// - userinfo fetch
    ///
    /// Requires `&mut self` so OIDC can swap out its client on JWKS rotation
    /// while holding the `write()` lock that the caller already holds.
    async fn exchange_code(&mut self, code: &str) -> Result<OAuthSession, anyhow::Error>;

    /// Refresh an existing access token using the credentials stored in the
    /// user's `OAuth` record.
    async fn refresh_token(
        &self,
        oauth: &OAuth,
        scope: Option<&str>,
        headers: HeaderMap,
    ) -> Result<Bearer, anyhow::Error>;

    /// Return the provider's logout / end-session URL, if one exists.
    fn logout_url(&self) -> Option<Url>;
}

// ── Output types ────────────────────────────────────────────────────────────

/// Everything produced by a successful code exchange.
#[derive(Debug)]
pub struct OAuthSession {
    /// Stored verbatim in `OAuth::bearer` in the user model.
    pub bearer: Bearer,
    pub claims: ProviderClaims,
    pub userinfo: ProviderUserInfo,
}

/// Identity claims extracted from the ID token / JWT.
#[derive(Debug, Clone)]
pub struct ProviderClaims {
    /// `sub` – stable unique identifier for the user at this provider.
    pub sub: Option<String>,
    pub email: Option<String>,
    pub name: Option<String>,
    /// Group memberships / roles declared by the provider.
    pub groups: HashSet<String>,
    /// Any additional claims not captured by the fields above.
    pub other: HashMap<String, serde_json::Value>,
}

/// Data returned by the provider's userinfo endpoint.
#[derive(Debug, Clone)]
pub struct ProviderUserInfo {
    pub sub: Option<String>,
    pub email: Option<String>,
    pub name: Option<String>,
    pub preferred_username: Option<String>,
    pub picture: Option<String>,
}
