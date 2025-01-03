use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::about::current;
use crate::handlers::http::modal::utils::rbac_utils::get_metadata;

use super::option::CONFIG;
use actix_web::dev::ServiceRequest;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio::runtime::Handle;
use tracing::info;
use tracing::{
    error,
    field::{Field, Visit},
    Event, Metadata, Subscriber,
};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};
use ulid::Ulid;
use url::Url;

pub struct AuditLayer {
    client: Arc<Client>,
    log_endpoint: Url,
    username: Option<String>,
    password: Option<String>,
    runtime_handle: Handle,
}

impl AuditLayer {
    /// Create an audit layer that works with the tracing system to capture
    /// and push audit logs to the appropriate logger over HTTP
    pub fn new(runtime_handle: Handle) -> Option<Self> {
        let audit_logger = CONFIG.parseable.audit_logger.as_ref()?;
        let client = Arc::new(reqwest::Client::new());
        let log_endpoint = match audit_logger.join("/api/v1/ingest") {
            Ok(url) => url,
            Err(err) => {
                error!("Couldn't setup audit logger: {err}");
                return None;
            }
        };

        let username = CONFIG.parseable.audit_username.clone();
        let password = CONFIG.parseable.audit_password.clone();

        Some(Self {
            client,
            log_endpoint,
            username,
            password,
            runtime_handle,
        })
    }
}

impl<S> Layer<S> for AuditLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, _: &Metadata<'_>, _: Context<'_, S>) -> bool {
        true // log everything if it is auditable
    }

    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut visitor = AuditVisitor::default();
        event.record(&mut visitor);

        // if the log line contains `audit` string with serialized json object, construct an HTTP request and push to configured audit endpoint
        // NOTE: We only support the ingest API of parseable for audit logging parseable
        if visitor.audit {
            let mut req = self
                .client
                .post(self.log_endpoint.as_str())
                .json(&visitor.json)
                .header("x-p-stream", "audit_log");
            if let Some(username) = self.username.as_ref() {
                req = req.basic_auth(username, self.password.as_ref())
            }

            self.runtime_handle.spawn(async move {
                match req.send().await {
                    Ok(r) => {
                        if let Err(e) = r.error_for_status() {
                            println!("{e}")
                        }
                    }
                    Err(e) => eprintln!("Failed to send audit event: {}", e),
                }
            });
        }
    }
}

#[derive(Debug, Default)]
struct AuditVisitor {
    json: Map<String, Value>,
    audit: bool,
}

impl Visit for AuditVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "audit" {
            if let Ok(Value::Object(json)) = serde_json::from_str(value) {
                self.audit = true;
                self.json = json;
            }
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn Debug) {}
}

#[non_exhaustive]
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum AuditLogVersion {
    V1 = 1,
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActorLog {
    pub remote_host: String,
    pub user_agent: String,
    pub username: String,
    pub authorization_method: String,
}

#[derive(Serialize, Default)]
pub struct RequestLog {
    pub method: String,
    pub path: String,
    pub protocol: String,
    pub headers: HashMap<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseLog {
    pub status_code: u16,
    pub error: Option<String>,
}

impl Default for ResponseLog {
    fn default() -> Self {
        // Server failed to respond
        ResponseLog {
            status_code: 500,
            error: None,
        }
    }
}

const DROP_HEADERS: [&str; 4] = ["authorization", "cookie", "user-agent", "x-p-stream"];

pub struct AuditLogBuilder {
    version: AuditLogVersion,
    pub deployment_id: Ulid,
    audit_id: Ulid,
    start_time: DateTime<Utc>,
    pub stream: String,
    pub actor: ActorLog,
    pub request: RequestLog,
    pub response: ResponseLog,
}

impl Default for AuditLogBuilder {
    fn default() -> Self {
        AuditLogBuilder {
            version: AuditLogVersion::V1,
            deployment_id: Ulid::nil(),
            audit_id: Ulid::new(),
            start_time: Utc::now(),
            stream: String::default(),
            actor: ActorLog::default(),
            request: RequestLog::default(),
            response: ResponseLog::default(),
        }
    }
}

impl AuditLogBuilder {
    pub async fn set_deployment_id(&mut self) {
        self.deployment_id = get_metadata().await.unwrap().deployment_id;
    }

    pub fn set_response_error(&mut self, err: String) {
        self.response.error = Some(err);
    }

    pub fn set_stream_name(&mut self, stream: String) {
        self.stream = stream;
    }

    pub fn update_from_http(&mut self, req: &ServiceRequest) {
        let conn = req.connection_info();

        self.request = RequestLog {
            method: req.method().to_string(),
            path: req.path().to_string(),
            protocol: conn.scheme().to_owned(),
            headers: req
                .headers()
                .iter()
                .filter_map(|(name, value)| match name.as_str() {
                    // NOTE: drop headers that are not required
                    name if DROP_HEADERS.contains(&name.to_lowercase().as_str()) => None,
                    name => {
                        // NOTE: Drop headers that can't be parsed as string
                        value
                            .to_str()
                            .map(|value| (name.to_owned(), value.to_string()))
                            .ok()
                    }
                })
                .collect(),
        };
        self.actor = ActorLog {
            remote_host: conn.realip_remote_addr().unwrap_or_default().to_owned(),
            user_agent: req
                .headers()
                .get("User-Agent")
                .and_then(|a| a.to_str().ok())
                .unwrap_or_default()
                .to_owned(),
            ..Default::default()
        }
    }
}

impl Drop for AuditLogBuilder {
    fn drop(&mut self) {
        let audit_json = json!({
            "version": self.version as u8,
            "parseableVersion": current().released_version.to_string(),
            "deploymentId" : self.deployment_id,
            "auditId" : self.audit_id,
            "startTime" : self.start_time.to_rfc3339(),
            "endTime" : Utc::now().to_rfc3339(),
            "stream" : self.stream,
            "actor" : self.actor,
            "request" : self.request,
            "response" : self.response,
        });
        info!(audit = audit_json.to_string())
    }
}
