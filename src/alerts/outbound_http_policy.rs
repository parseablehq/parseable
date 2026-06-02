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
use url::Url;

#[derive(Debug, Clone, Copy)]
pub enum AlertTargetKind {
    Slack,
    Webhook,
    AlertManager,
}

// policy for alert-target egress
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertTargetPolicyConfig {
    pub allow_private: bool,
    pub allowed_domains: Vec<String>,
    pub allowed_cidrs: Vec<String>,
    pub denied_domains: Vec<String>,
    pub denied_cidrs: Vec<String>,
    pub allow_invalid_tls: bool,
}

impl Default for AlertTargetPolicyConfig {
    fn default() -> Self {
        Self {
            allow_private: false,
            allowed_domains: vec![],
            allowed_cidrs: vec![],
            denied_domains: vec![],
            denied_cidrs: vec![],
            allow_invalid_tls: false,
        }
    }
}

pub static ALERT_TARGET_POLICY: Lazy<RwLock<AlertTargetPolicyConfig>> =
    Lazy::new(|| RwLock::new(AlertTargetPolicyConfig::default()));

// Read one policy snapshot per validation
pub async fn active_policy() -> AlertTargetPolicyConfig {
    ALERT_TARGET_POLICY.read().await.clone()
}

// Replace atomically only after validation
pub async fn replace_policy(policy: AlertTargetPolicyConfig) -> Result<(), OutboundPolicyError> {
    validate_policy(&policy)?;
    *ALERT_TARGET_POLICY.write().await = policy;
    Ok(())
}

// Validate admin-supplied policy before it is stored or used for target checks.
pub fn validate_policy(policy: &AlertTargetPolicyConfig) -> Result<(), OutboundPolicyError> {
    parse_cidrs(&policy.allowed_cidrs)?;
    parse_cidrs(&policy.denied_cidrs)?;
    Ok(())
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
}

// All alert-target outbound networking must enter here.
// The policy authorizes the resolved destination, then pins that exact DNS result into reqwest
pub async fn prepare_alert_target(
    endpoint: &Url,
    kind: AlertTargetKind,
    skip_tls_check: bool,
    headers: Option<&HashMap<String, String>>,
) -> Result<PreparedAlertTarget, OutboundPolicyError> {
    validate_scheme(endpoint, kind)?;
    let policy = active_policy().await;
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
    })
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
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    domains.iter().any(|domain| {
        let domain = domain.trim_end_matches('.').to_ascii_lowercase();
        host == domain || host.ends_with(&format!(".{domain}"))
    })
}
