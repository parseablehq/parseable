use http::{HeaderMap, HeaderValue, header::HeaderName};
use ipnet::IpNet;
use reqwest::ClientBuilder;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};
use tokio::net::lookup_host;
use url::Url;

#[derive(Debug, Clone, Copy)]
pub enum AlertTargetKind {
    Slack,
    Webhook,
    AlertManager,
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

    #[error("private target requires P_ALERT_TARGET_ALLOW_PRIVATE=true and an allowlist match:{0}")]
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

// All Alert-target outbound networking must enter here
// Policy authorizes the resolved destination, then pins exact DNS result into reqwest
pub async fn prepare_alert_target(
    endpoint: &Url,
    kind: AlertTargetKind,
    skip_tls_check: bool,
    headers: Option<&HashMap<String, String>>,
) -> Result<PreparedAlertTarget, OutboundPolicyError> {
    validate_scheme(endpoint, kind)?;
    validate_tls_policy(kind, skip_tls_check)?;
    let host = endpoint
        .host_str()
        .ok_or(OutboundPolicyError::MissingHost)?
        .to_string();
    validate_domain_policy(&host, kind)?;
    let port = endpoint
        .port_or_known_default()
        .ok_or_else(|| OutboundPolicyError::UnsupportedScheme(endpoint.scheme().to_string()))?;
    let resolved_addrs = resolve_endpoint_addrs(&host, port).await?;
    validate_resolved_addrs(&host, &resolved_addrs)?;
    let authorization_allowed = operator_allowlist_matches(&host, &resolved_addrs)?;
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

// create http client with timeouts to prevent resource exhaustion
fn default_client_builder() -> ClientBuilder {
    ClientBuilder::new()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(8)
        .use_rustls_tls()
        .http1_only()
}

// We Only Accept http & https scheme
fn validate_scheme(endpoint: &Url, kind: AlertTargetKind) -> Result<(), OutboundPolicyError> {
    match (kind, endpoint.scheme()) {
        // for slack we must use https
        (AlertTargetKind::Slack, "https") => Ok(()),
        (AlertTargetKind::Slack, _) => Err(OutboundPolicyError::SlackRequiresHttps),
        (_, "http" | "https") => Ok(()),
        (_, scheme) => Err(OutboundPolicyError::UnsupportedScheme(scheme.to_string())),
    }
}

fn validate_tls_policy(
    kind: AlertTargetKind,
    skip_tls_check: bool,
) -> Result<(), OutboundPolicyError> {
    // for slack we must ensure tls is enabled
    if matches!(kind, AlertTargetKind::Slack) && skip_tls_check {
        return Err(OutboundPolicyError::InvalidTlsDisabled);
    }
    // both tls flags need to align
    if skip_tls_check && !env_bool("P_ALERT_TARGET_ALLOW_INVALID_TLS", false) {
        return Err(OutboundPolicyError::InvalidTlsDisabled);
    }
    Ok(())
}

fn validate_domain_policy(host: &str, kind: AlertTargetKind) -> Result<(), OutboundPolicyError> {
    // block explicitly denied domains
    if matches_domain_list(host, "P_ALERT_TARGET_DENIED_DOMAINS") {
        return Err(OutboundPolicyError::DeniedDomain(host.to_string()));
    }
    // slack target restricted to official webhook domains only
    if matches!(kind, AlertTargetKind::Slack)
        && host != "hooks.slack.com"
        && host != "hooks.slack-gov.com"
    {
        return Err(OutboundPolicyError::InvalidSlackHost(host.to_string()));
    }
    Ok(())
}

// resolve hostname to ip
async fn resolve_endpoint_addrs(
    host: &str,
    port: u16,
) -> Result<Vec<SocketAddr>, OutboundPolicyError> {
    // if host is ip, create socket directly
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    // perform async dns resolution
    let addrs = lookup_host((host, port))
        .await
        .map_err(|source| OutboundPolicyError::ResolveFailed {
            host: host.to_string(),
            source,
        })?
        .collect::<Vec<_>>();
    // fail if no address found
    if addrs.is_empty() {
        return Err(OutboundPolicyError::NoResolvedAddresses(host.to_string()));
    }
    Ok(addrs)
}
// check if target is explicitly allowlisted for private address access
fn operator_allowlist_matches(
    host: &str,
    addrs: &[SocketAddr],
) -> Result<bool, OutboundPolicyError> {
    let allowed_cidrs = cidrs_from_env("P_ALERT_TARGET_ALLOWED_CIDRS")?;
    let domain_allowed = matches_domain_list(host, "P_ALERT_TARGET_ALLOWED_DOMAINS");
    let cidrs_allowed = addrs
        .iter()
        .any(|addr| allowed_cidrs.iter().any(|cidr| cidr.contains(&addr.ip())));
    Ok(domain_allowed || cidrs_allowed)
}

fn validate_resolved_addrs(host: &str, addrs: &[SocketAddr]) -> Result<(), OutboundPolicyError> {
    let denied_cidrs = cidrs_from_env("P_ALERT_TARGET_DENIED_CIDRS")?;
    let private_allowed = env_bool("P_ALERT_TARGET_ALLOW_PRIVATE", false);
    let explicitly_allowlisted = operator_allowlist_matches(host, addrs)?;
    for addr in addrs {
        let ip = addr.ip();
        // deny ip addr which  fall in the cidr range
        if denied_cidrs.iter().any(|cidr| cidr.contains(&ip)) {
            return Err(OutboundPolicyError::DeniedAddress(ip));
        }
        // Private addresses require explicit opt-in plus allowlist match
        if builtin_denied_ip(ip) && !(private_allowed && explicitly_allowlisted) {
            return Err(OutboundPolicyError::PrivateAddressNotAllowed(ip));
        }
    }
    Ok(())
}

fn builtin_denied_ip(ip: IpAddr) -> bool {
    // route to appropriate ip validation
    match ip {
        IpAddr::V4(ip) => denied_ipv4(ip),
        IpAddr::V6(ip) => denied_ipv6(ip),
    }
}

fn denied_ipv4(ip: Ipv4Addr) -> bool {
    // Check against RFC-defined special-use IPv4 address ranges
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
    // Check against RFC-defined special-use IPv6 address ranges
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
        // Block headers that can bypass security controls or proxy behavior
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
    // These headers are always blocked because they control connection routing or proxy behavior
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

    // Authorization header is only allowed for explicitly allowlisted destinations.
    let authorization_denied = name == "authorization" && !authorization_allowed;
    always_denied || authorization_denied
}

// parse cidr blocks from env for ip allowlist/denylist
fn cidrs_from_env(name: &str) -> Result<Vec<IpNet>, OutboundPolicyError> {
    env_list(name)
        .into_iter()
        .map(|value| {
            value
                .parse::<IpNet>()
                .map_err(|source| OutboundPolicyError::InvalidCidr { value, source })
        })
        .collect()
}

// check if host matches allowed domain,supporting subdomain
fn matches_domain_list(host: &str, env_name: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    env_list(env_name).into_iter().any(|domain| {
        let domain = domain.trim_end_matches('.').to_ascii_lowercase();
        host == domain || host.ends_with(&format!(".{domain}"))
    })
}

// read comma-seperated values from env for config parsing
fn env_list(name: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

// read bool values from env
fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(default)
}
