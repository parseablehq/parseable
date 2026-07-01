use http::{HeaderMap, HeaderValue, header::HeaderName};
use ipnet::IpNet;
use once_cell::sync::Lazy;
use reqwest::ClientBuilder;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};
use tokio::net::lookup_host;
use tokio::sync::RwLock;
use tracing::error;
use url::Url;

use crate::metastore::{MetastoreError, metastore_traits::Metastore};
use crate::parseable::PARSEABLE;

#[derive(Debug, Clone, Copy)]
pub enum AlertTargetKind {
    Slack,
    Webhook,
    AlertManager,
}

// policy for alert-target egress
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertTargetPolicyConfig {
    pub allow_private: bool,
    pub allowed_domains: Vec<String>,
    pub allowed_cidrs: Vec<String>,
    pub denied_domains: Vec<String>,
    pub denied_cidrs: Vec<String>,
    pub allow_invalid_tls: bool,
}

pub static ALERT_TARGET_POLICY: Lazy<RwLock<HashMap<String, AlertTargetPolicyConfig>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

// Read one policy snapshot per validation
pub async fn active_policy(
    tenant_id: &Option<String>,
) -> Result<AlertTargetPolicyConfig, OutboundPolicyError> {
    let tenant = tenant_id
        .as_deref()
        .unwrap_or(crate::parseable::DEFAULT_TENANT);
    let guard = ALERT_TARGET_POLICY.read().await;
    Ok(guard
        .get(tenant)
        .cloned()
        .unwrap_or_else(AlertTargetPolicyConfig::default))
}

// Replace atomically only after validation
pub async fn replace_policy(
    tenant_id: &Option<String>,
    policy: AlertTargetPolicyConfig,
) -> Result<(), OutboundPolicyError> {
    let tenant = tenant_id
        .as_deref()
        .unwrap_or(crate::parseable::DEFAULT_TENANT);
    validate_policy(&policy)?;
    let storage_tenant = if tenant == crate::parseable::DEFAULT_TENANT {
        None
    } else {
        Some(tenant.to_string())
    };
    PARSEABLE
        .metastore
        .put_outbound_policy(&storage_tenant, &policy)
        .await?;
    ALERT_TARGET_POLICY
        .write()
        .await
        .insert(tenant.to_string(), policy);
    Ok(())
}

// Validate admin-supplied policy before it is stored or used for target checks.
pub fn validate_policy(policy: &AlertTargetPolicyConfig) -> Result<(), OutboundPolicyError> {
    let allowed = parse_cidrs(&policy.allowed_cidrs)?;
    let denied = parse_cidrs(&policy.denied_cidrs)?;

    if let Some(conflict) = find_conflicting_cidr(&allowed, &denied) {
        return Err(OutboundPolicyError::ConflictingCidrs(conflict));
    }

    if let Some(conflict) = find_conflicting_domain(&policy.allowed_domains, &policy.denied_domains)
    {
        return Err(OutboundPolicyError::ConflictingDomains(conflict));
    }

    Ok(())
}

fn find_conflicting_cidr(allowed: &[IpNet], denied: &[IpNet]) -> Option<String> {
    allowed.iter().find_map(|allowed_cidr| {
        denied
            .iter()
            .any(|denied_cidr| cidrs_overlap(allowed_cidr, denied_cidr))
            .then_some(allowed_cidr.to_string())
    })
}

fn find_conflicting_domain(allowed: &[String], denied: &[String]) -> Option<String> {
    allowed.iter().find_map(|allowed_domain| {
        denied
            .iter()
            .any(|denied_domain| domains_overlap(allowed_domain, denied_domain))
            .then_some(normalize_domain(allowed_domain))
    })
}

fn normalize_domain(domain: &str) -> String {
    domain.trim_end_matches('.').to_ascii_lowercase()
}

fn domains_overlap(left: &str, right: &str) -> bool {
    let left = normalize_domain(left);
    let right = normalize_domain(right);

    domain_matches_or_contains(&left, &right) || domain_matches_or_contains(&right, &left)
}

fn domain_matches_or_contains(candidate: &str, parent: &str) -> bool {
    if candidate == parent {
        return true;
    }

    candidate
        .strip_suffix(parent)
        .is_some_and(|prefix| prefix.ends_with('.'))
}

fn cidrs_overlap(left: &IpNet, right: &IpNet) -> bool {
    left.contains(right) || right.contains(left)
}

fn parse_cidrs(values: &[String]) -> Result<Vec<IpNet>, OutboundPolicyError> {
    values
        .iter()
        .map(|val| {
            val.parse::<IpNet>()
                .map_err(|source| OutboundPolicyError::InvalidCidr {
                    value: val.clone(),
                    source,
                })
        })
        .collect()
}

pub struct PreparedAlertTarget {
    pub client: reqwest::Client,
    pub headers: http::HeaderMap,
    pub authorization_allowed: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum OutboundPolicyError {
    #[error("missing URL host")]
    MissingHost,

    #[error("Slack alert target must use HTTPS")]
    SlackRequiresHttps,

    #[error("unsupported URL scheme:{0}")]
    UnsupportedScheme(String),

    #[error("skipTlsCheck is disabled by server policy")]
    InvalidTlsDisabled,

    #[error("failed to resolve target host {host} :{source}")]
    ResolveFailed {
        host: String,
        source: std::io::Error,
    },
    #[error("target host resolved to no address: {0}")]
    NoResolvedAddresses(String),

    #[error("target address is denied by outbound policy: {0}")]
    DeniedAddress(IpAddr),

    #[error("target domain is denied by outbound policy: {0}")]
    DeniedDomain(String),

    #[error("private target requires allowPrivate=true and an allowlist match:{0}")]
    PrivateAddressNotAllowed(IpAddr),

    #[error("invalid outbound policy CIDR{value}:{source}")]
    InvalidCidr {
        value: String,
        source: ipnet::AddrParseError,
    },

    #[error("custom header is not allowed:{0}")]
    DeniedHeader(String),

    #[error("invalid custom header name:{0}")]
    InvalidHeaderName(String),

    #[error("invalid custom header value for:{0}")]
    InvalidHeaderValue(String),

    #[error("invalid slack host: {0}")]
    InvalidSlackHost(String),

    #[error("tenant id is required for outbound policy")]
    MissingTenant,

    #[error("outbound policy not found for tenant: {0}")]
    PolicyNotFound(String),

    #[error("allow and deny CIDR lists conflict on: {0}")]
    ConflictingCidrs(String),

    #[error("allow and deny domain lists conflict on: {0}")]
    ConflictingDomains(String),

    #[error("failed to load outbound HTTP policy for tenant {tenant_id}: {source}")]
    PolicyValidationFailed {
        tenant_id: String,
        #[source]
        source: Box<OutboundPolicyError>,
    },

    #[error("metastore error: {0}")]
    Metastore(#[from] MetastoreError),
}

// All alert-target outbound networking must enter here.
// The policy authorizes the resolved destination, then pins that exact DNS result into reqwest
pub async fn prepare_alert_target(
    tenant_id: &Option<String>,
    endpoint: &Url,
    kind: AlertTargetKind,
    skip_tls_check: bool,
    headers: Option<&HashMap<String, String>>,
) -> Result<PreparedAlertTarget, OutboundPolicyError> {
    validate_scheme(endpoint, kind)?;
    let policy = active_policy(tenant_id).await?;
    validate_tls_policy(kind, skip_tls_check, &policy)?;
    let host = endpoint
        .host_str()
        .ok_or(OutboundPolicyError::MissingHost)?
        .to_string();
    validate_domain_policy(&host, kind, &policy)?;
    let port = endpoint
        .port_or_known_default()
        .ok_or_else(|| OutboundPolicyError::UnsupportedScheme(endpoint.scheme().to_string()))?;
    let resolved_addrs = resolve_endpoint_addrs(&host, port).await?;
    validate_resolved_addrs(&host, &resolved_addrs, &policy)?;
    let authorization_allowed = operator_allowlist_matches(&host, &resolved_addrs, &policy)?;
    let validated_headers = validate_header(headers, authorization_allowed)?;
    let mut builder = default_client_builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .resolve_to_addrs(&host, &resolved_addrs);
    if skip_tls_check {
        builder = builder.danger_accept_invalid_certs(true);
    }
    let client = builder
        .build()
        .expect("alert target HTTP client can be constructed");
    Ok(PreparedAlertTarget {
        client,
        headers: validated_headers,
        authorization_allowed,
    })
}

pub async fn load_policy_for_tenant(
    metastore: &dyn Metastore,
    tenant_id: &str,
) -> Result<(), OutboundPolicyError> {
    let policy = metastore.get_outbound_policy(tenant_id).await?;
    validate_policy(&policy)?;
    ALERT_TARGET_POLICY
        .write()
        .await
        .insert(tenant_id.to_string(), policy);
    Ok(())
}

pub async fn load_all_policies(metastore: &dyn Metastore) -> Result<(), OutboundPolicyError> {
    let policies = metastore.get_outbound_policies().await?;
    let loaded = validate_loaded_policies(policies)?;

    *ALERT_TARGET_POLICY.write().await = loaded;

    Ok(())
}

fn validate_loaded_policies(
    policies: HashMap<String, AlertTargetPolicyConfig>,
) -> Result<HashMap<String, AlertTargetPolicyConfig>, OutboundPolicyError> {
    let mut loaded = HashMap::new();
    for (tenant_id, policy) in policies {
        if let Err(err) = validate_policy(&policy) {
            error!("Failed to load outbound HTTP policy for tenant {tenant_id}: {err}");
            return Err(OutboundPolicyError::PolicyValidationFailed {
                tenant_id,
                source: Box::new(err),
            });
        }
        loaded.insert(tenant_id, policy);
    }
    Ok(loaded)
}

// Keep alert delivery bounded so a slow or stuck target cannot hold workers forever.
fn default_client_builder() -> ClientBuilder {
    ClientBuilder::new()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(8)
        .use_rustls_tls()
        .http1_only()
}

fn validate_scheme(endpoint: &Url, kind: AlertTargetKind) -> Result<(), OutboundPolicyError> {
    match (kind, endpoint.scheme()) {
        (AlertTargetKind::Slack, "https") => Ok(()),
        (AlertTargetKind::Slack, _) => Err(OutboundPolicyError::SlackRequiresHttps),
        (_, "http" | "https") => Ok(()),
        (_, scheme) => Err(OutboundPolicyError::UnsupportedScheme(scheme.to_string())),
    }
}

fn validate_tls_policy(
    kind: AlertTargetKind,
    skip_tls_check: bool,
    policy: &AlertTargetPolicyConfig,
) -> Result<(), OutboundPolicyError> {
    // Slack webhooks are always HTTPS-only; do not let per-target config weaken that.
    if matches!(kind, AlertTargetKind::Slack) && skip_tls_check {
        return Err(OutboundPolicyError::InvalidTlsDisabled);
    }
    // Invalid TLS is a deployment-level exception, not a user-controlled toggle.
    if skip_tls_check && !policy.allow_invalid_tls {
        return Err(OutboundPolicyError::InvalidTlsDisabled);
    }
    Ok(())
}

fn validate_domain_policy(
    host: &str,
    kind: AlertTargetKind,
    policy: &AlertTargetPolicyConfig,
) -> Result<(), OutboundPolicyError> {
    // Denied domains win before DNS resolution, which avoids needless egress.
    if matches_domain_list(host, &policy.denied_domains) {
        return Err(OutboundPolicyError::DeniedDomain(host.to_string()));
    }
    // Slack targets are not generic webhooks; keep them pinned to Slack-owned hosts.
    if matches!(kind, AlertTargetKind::Slack)
        && host != "hooks.slack.com"
        && host != "hooks.slack-gov.com"
    {
        return Err(OutboundPolicyError::InvalidSlackHost(host.to_string()));
    }
    Ok(())
}

async fn resolve_endpoint_addrs(
    host: &str,
    port: u16,
) -> Result<Vec<SocketAddr>, OutboundPolicyError> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    let addrs = lookup_host((host, port))
        .await
        .map_err(|source| OutboundPolicyError::ResolveFailed {
            host: host.to_string(),
            source,
        })?
        .collect::<Vec<_>>();
    if addrs.is_empty() {
        return Err(OutboundPolicyError::NoResolvedAddresses(host.to_string()));
    }
    Ok(addrs)
}

// Private/internal targets must match admin-owned allowlists before they may be used.
fn operator_allowlist_matches(
    host: &str,
    addrs: &[SocketAddr],
    policy: &AlertTargetPolicyConfig,
) -> Result<bool, OutboundPolicyError> {
    let allowed_cidrs = parse_cidrs(&policy.allowed_cidrs)?;
    let domain_allowed = matches_domain_list(host, &policy.allowed_domains);
    let cidrs_allowed = addrs
        .iter()
        .any(|addr| allowed_cidrs.iter().any(|cidr| cidr.contains(&addr.ip())));
    Ok(domain_allowed || cidrs_allowed)
}

// Fail closed for multi-address DNS responses. If any resolved address is
// denied or non-public without an allowlist match, reject the target.
fn validate_resolved_addrs(
    host: &str,
    addrs: &[SocketAddr],
    policy: &AlertTargetPolicyConfig,
) -> Result<(), OutboundPolicyError> {
    let denied_cidrs = parse_cidrs(&policy.denied_cidrs)?;
    let explicitly_allowlisted = operator_allowlist_matches(host, addrs, policy)?;
    for addr in addrs {
        let ip = addr.ip();
        if denied_cidrs.iter().any(|cidr| cidr.contains(&ip)) {
            return Err(OutboundPolicyError::DeniedAddress(ip));
        }
        if builtin_denied_ip(ip) && !(policy.allow_private && explicitly_allowlisted) {
            return Err(OutboundPolicyError::PrivateAddressNotAllowed(ip));
        }
    }
    Ok(())
}

fn builtin_denied_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => denied_ipv4(ip),
        IpAddr::V6(ip) => denied_ipv6(ip),
    }
}

fn denied_ipv4(ip: Ipv4Addr) -> bool {
    // Covers private, loopback, link-local, multicast, carrier NAT, and reserved ranges.
    ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1])
        || ip.octets()[0] >= 240
        || ip == Ipv4Addr::new(255, 255, 255, 255)
}

fn denied_ipv6(ip: Ipv6Addr) -> bool {
    // Covers loopback, link-local, unique-local, multicast, and mapped IPv4 ranges.
    if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
        return true;
    }
    let first = ip.segments()[0];

    if (first & 0xfe00) == 0xfc00 {
        return true;
    }
    if (first & 0xffc0) == 0xfe80 {
        return true;
    }

    // Block IPv6 transition ranges that can carry embedded IPv4 destinations.
    if first == 0x2002 {
        return true;
    }
    if first == 0x2001 && ip.segments()[1] == 0x0000 {
        return true;
    }

    if let Some(mapped) = ip.to_ipv4_mapped() {
        return denied_ipv4(mapped);
    }
    false
}

fn validate_header(
    headers: Option<&HashMap<String, String>>,
    authorization_allowed: bool,
) -> Result<HeaderMap, OutboundPolicyError> {
    let mut map = HeaderMap::new();
    let Some(headers) = headers else {
        return Ok(map);
    };
    for (name, value) in headers {
        let normalized = name.to_ascii_lowercase();
        // Block headers that can bypass connection policy or smuggle credentials.
        if denied_header(&normalized, authorization_allowed) {
            return Err(OutboundPolicyError::DeniedHeader(name.clone()));
        }
        // Validate header name/value are syntactically correct before insertion
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| OutboundPolicyError::InvalidHeaderName(name.clone()))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|_| OutboundPolicyError::InvalidHeaderValue(name.clone()))?;
        map.insert(header_name, header_value);
    }
    Ok(map)
}

fn denied_header(name: &str, authorization_allowed: bool) -> bool {
    // These headers are always blocked because they control routing or proxy behavior.
    let always_denied = matches!(
        name,
        "host"
            | "content-length"
            | "transfer-encoding"
            | "connection"
            | "upgrade"
            | "proxy-authorization"
            | "proxy-authenticate"
            | "cookie"
    );

    // Authorization is allowed only after the destination matches admin policy.
    let authorization_denied = name == "authorization" && !authorization_allowed;
    always_denied || authorization_denied
}

// Domain entries match the exact host and its subdomains.
fn matches_domain_list(host: &str, domains: &[String]) -> bool {
    let host = normalize_domain(host);
    domains.iter().any(|domain| {
        let domain = normalize_domain(domain);
        host == domain || host.ends_with(&format!(".{domain}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy_with(
        allow_private: bool,
        allowed_cidrs: &[&str],
        denied_cidrs: &[&str],
    ) -> AlertTargetPolicyConfig {
        AlertTargetPolicyConfig {
            allow_private,
            allowed_cidrs: allowed_cidrs
                .iter()
                .map(|value| value.to_string())
                .collect(),
            denied_cidrs: denied_cidrs.iter().map(|value| value.to_string()).collect(),
            ..Default::default()
        }
    }

    fn socket(ip: IpAddr) -> SocketAddr {
        SocketAddr::new(ip, 80)
    }

    #[test]
    fn public_addresses_are_allowed_by_default() {
        let policy = AlertTargetPolicyConfig::default();
        let addrs = [socket(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)))];

        validate_resolved_addrs("example.com", &addrs, &policy).unwrap();
    }

    #[test]
    fn private_addresses_are_blocked_by_default() {
        let policy = AlertTargetPolicyConfig::default();
        let addrs = [socket(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)))];

        let err = validate_resolved_addrs("localhost", &addrs, &policy).unwrap_err();

        assert!(matches!(
            err,
            OutboundPolicyError::PrivateAddressNotAllowed(_)
        ));
    }

    #[test]
    fn private_addresses_require_allow_private_and_allowlist_match() {
        let addrs = [socket(IpAddr::V4(Ipv4Addr::new(10, 10, 0, 10)))];
        let policy = policy_with(true, &["10.10.0.0/16"], &[]);

        validate_resolved_addrs("internal.example.com", &addrs, &policy).unwrap();
    }

    #[test]
    fn denied_cidrs_override_allowlists() {
        let addrs = [socket(IpAddr::V4(Ipv4Addr::new(10, 10, 0, 10)))];
        let policy = policy_with(true, &["10.10.0.0/16"], &["10.10.0.10/32"]);

        let err = validate_resolved_addrs("internal.example.com", &addrs, &policy).unwrap_err();

        assert!(matches!(err, OutboundPolicyError::DeniedAddress(_)));
    }

    #[test]
    fn invalid_cidr_policy_is_rejected() {
        let policy = policy_with(true, &["bad-cidr"], &[]);

        let err = validate_policy(&policy).unwrap_err();

        assert!(matches!(err, OutboundPolicyError::InvalidCidr { .. }));
    }

    #[test]
    fn authorization_header_requires_allowlist_decision() {
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer token".to_string());

        let err = validate_header(Some(&headers), false).unwrap_err();
        assert!(matches!(err, OutboundPolicyError::DeniedHeader(name) if name == "Authorization"));

        let validated = validate_header(Some(&headers), true).unwrap();
        assert_eq!(
            validated.get("authorization").unwrap(),
            HeaderValue::from_static("Bearer token")
        );
    }

    #[test]
    fn cookie_header_is_always_blocked() {
        let mut headers = HashMap::new();
        headers.insert("Cookie".to_string(), "session=secret".to_string());

        let err = validate_header(Some(&headers), true).unwrap_err();

        assert!(matches!(err, OutboundPolicyError::DeniedHeader(name) if name == "Cookie"));
    }

    #[test]
    fn slack_targets_are_https_only_and_host_pinned() {
        let http_slack = Url::parse("http://hooks.slack.com/services/test").unwrap();
        let fake_slack = Url::parse("https://example.com/services/test").unwrap();
        let policy = AlertTargetPolicyConfig::default();

        assert!(matches!(
            validate_scheme(&http_slack, AlertTargetKind::Slack),
            Err(OutboundPolicyError::SlackRequiresHttps)
        ));
        assert!(matches!(
            validate_domain_policy("example.com", AlertTargetKind::Slack, &policy),
            Err(OutboundPolicyError::InvalidSlackHost(_))
        ));
        validate_domain_policy("hooks.slack.com", AlertTargetKind::Slack, &policy).unwrap();
        validate_domain_policy("hooks.slack-gov.com", AlertTargetKind::Slack, &policy).unwrap();
        validate_scheme(&fake_slack, AlertTargetKind::Slack).unwrap();
    }

    #[test]
    fn ipv6_transition_addresses_are_blocked() {
        assert!(builtin_denied_ip(IpAddr::V6(
            "2002:0a0a:0001::".parse().unwrap()
        )));
        assert!(builtin_denied_ip(IpAddr::V6(
            "2001:0000:4136:e378:8000:63bf:3fff:fdd2".parse().unwrap()
        )));
    }

    #[test]
    fn conflicting_allow_and_deny_cidrs_are_rejected() {
        let policy = policy_with(true, &["10.0.0.0/8"], &["10.1.0.0/16"]);
        let err = validate_policy(&policy).unwrap_err();
        assert!(matches!(err, OutboundPolicyError::ConflictingCidrs(_)));
    }

    #[test]
    fn conflicting_allow_and_deny_cidrs_are_rejected_regardless_of_order() {
        let policy = policy_with(true, &["10.1.0.0/16"], &["10.0.0.0/8"]);
        let err = validate_policy(&policy).unwrap_err();
        assert!(matches!(err, OutboundPolicyError::ConflictingCidrs(_)));
    }

    #[test]
    fn non_overlapping_cidrs_are_allowed() {
        let policy = policy_with(true, &["10.0.0.0/24"], &["10.0.1.0/24"]);
        validate_policy(&policy).unwrap();
    }

    #[test]
    fn conflicting_allow_and_deny_domains_are_rejected() {
        let policy = AlertTargetPolicyConfig {
            allowed_domains: vec!["Example.com.".to_string()],
            denied_domains: vec!["sub.example.com".to_string()],
            ..Default::default()
        };

        let err = validate_policy(&policy).unwrap_err();

        assert!(
            matches!(err, OutboundPolicyError::ConflictingDomains(domain) if domain == "example.com")
        );
    }

    #[test]
    fn conflicting_allow_and_deny_domains_are_rejected_regardless_of_order() {
        let policy = AlertTargetPolicyConfig {
            allowed_domains: vec!["sub.example.com".to_string()],
            denied_domains: vec!["example.com".to_string()],
            ..Default::default()
        };

        let err = validate_policy(&policy).unwrap_err();

        assert!(
            matches!(err, OutboundPolicyError::ConflictingDomains(domain) if domain == "sub.example.com")
        );
    }

    #[test]
    fn similar_but_unrelated_domains_are_allowed() {
        let policy = AlertTargetPolicyConfig {
            allowed_domains: vec!["example.com".to_string()],
            denied_domains: vec!["notexample.com".to_string()],
            ..Default::default()
        };

        validate_policy(&policy).unwrap();
    }

    #[tokio::test]
    async fn active_policy_defaults_for_missing_tenant() {
        let tenant_id = Some("tenant-without-policy".to_string());

        let policy = active_policy(&tenant_id).await.unwrap();

        assert!(!policy.allow_private);
        assert!(policy.allowed_domains.is_empty());
        assert!(policy.allowed_cidrs.is_empty());
        assert!(policy.denied_domains.is_empty());
        assert!(policy.denied_cidrs.is_empty());
        assert!(!policy.allow_invalid_tls);
    }

    #[test]
    fn validate_loaded_policies_rejects_invalid_entries() {
        let mut policies = HashMap::new();
        policies.insert(
            "invalid".to_string(),
            AlertTargetPolicyConfig {
                allowed_cidrs: vec!["bad-cidr".to_string()],
                ..Default::default()
            },
        );

        let err = validate_loaded_policies(policies).unwrap_err();

        assert!(matches!(
            err,
            OutboundPolicyError::PolicyValidationFailed { tenant_id, .. } if tenant_id == "invalid"
        ));
    }
}
