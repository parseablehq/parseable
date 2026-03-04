use async_trait::async_trait;
use openid::{Bearer, Options, Token};
use url::Url;

use crate::{
    handlers::http::{API_BASE_PATH, API_VERSION},
    oauth::provider::{OAuthProvider, OAuthSession, ProviderClaims, ProviderUserInfo},
    oidc::{Claims, DiscoveredClient, OpenidConfig},
    rbac::user::OAuth,
};

/// Wraps the OpenID Connect `DiscoveredClient`.
///
/// Stores the original `OpenidConfig` and the redirect suffix so that it can
/// reconnect (rotating the JWKS) inside `exchange_code` without any outside
/// help.
#[derive(Debug)]
pub struct GlobalClient {
    client: DiscoveredClient,
    /// Original config – cloned and used to reconnect on JWKS rotation.
    config: OpenidConfig,
    /// `"api/v1/o/code"` – the path appended to the base URL for the
    /// redirect URI when re-discovering.
    redirect_suffix: String,
}

impl GlobalClient {
    pub fn new(client: DiscoveredClient, config: OpenidConfig, redirect_suffix: String) -> Self {
        Self {
            client,
            config,
            redirect_suffix,
        }
    }
}

#[async_trait]
impl OAuthProvider for GlobalClient {
    fn auth_url(&self, scope: &str, state: Option<String>) -> Url {
        self.client.auth_url(&Options {
            scope: Some(scope.to_string()),
            state,
            ..Default::default()
        })
    }

    /// Exchange an authorization code for the full session, handling JWKS
    /// rotation transparently: if `decode_token` fails with the cached client,
    /// a fresh discovery is performed and decoding is retried once.
    async fn exchange_code(&mut self, code: &str) -> Result<OAuthSession, anyhow::Error> {
        let mut token: Token<Claims> = self.client.request_token(code).await?.into();

        let id_token = token
            .id_token
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("OIDC provider did not return an id_token"))?;

        if let Err(e) = self.client.decode_token(id_token) {
            // Stale JWKS – reconnect and retry once.
            tracing::warn!("id_token decode failed ({e}), rotating JWKS and retrying");
            self.client = self.config.clone().connect(&self.redirect_suffix).await?;
            self.client.decode_token(id_token)?;
        }

        self.client.validate_token(id_token, None, None)?;

        let raw_claims = id_token
            .payload()
            .expect("token is decoded at this point")
            .clone();

        // `sub` is a required non-optional String in StandardClaims.
        // `email` and `name` are not in StandardClaims (they're userinfo fields)
        // but providers sometimes include them as additional claims in the ID
        // token; extract them from `other` if present.
        let groups: std::collections::HashSet<String> = raw_claims
            .other
            .get("groups")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let claims = ProviderClaims {
            sub: Some(raw_claims.standard.sub.clone()),
            email: raw_claims
                .other
                .get("email")
                .and_then(|v| v.as_str())
                .map(String::from),
            name: raw_claims
                .other
                .get("name")
                .and_then(|v| v.as_str())
                .map(String::from),
            groups,
            other: raw_claims.other.clone(),
        };

        let userinfo_raw = self.client.request_userinfo(&token).await?;
        let userinfo = ProviderUserInfo {
            sub: userinfo_raw.sub.clone(),
            email: userinfo_raw.email.clone(),
            name: userinfo_raw.name.clone(),
            preferred_username: userinfo_raw.preferred_username.clone(),

            picture: userinfo_raw
                .picture
                .as_ref()
                .map(|p| p.as_str().to_string()),
        };

        Ok(OAuthSession {
            bearer: token.bearer,
            claims,
            userinfo,
        })
    }

    async fn refresh_token(
        &self,
        oauth: &OAuth,
        scope: Option<&str>,
    ) -> Result<Bearer, anyhow::Error> {
        // Box the clone so we can pass it to the openid client.
        let boxed: Box<OAuth> = Box::new(oauth.clone());
        Ok(self.client.refresh_token(boxed, scope).await?)
    }

    fn logout_url(&self) -> Option<Url> {
        self.client.config().end_session_endpoint.clone()
    }
}

/// Runs OIDC discovery and wraps the result in a `GlobalClient`.
pub async fn connect_oidc(config: OpenidConfig) -> Result<GlobalClient, openid::error::Error> {
    let redirect_suffix = format!("{API_BASE_PATH}/{API_VERSION}/o/code");
    let client = config.clone().connect(&redirect_suffix).await?;
    Ok(GlobalClient::new(client, config, redirect_suffix))
}
