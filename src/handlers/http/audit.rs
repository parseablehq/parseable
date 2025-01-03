use super::middleware::Message;
use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    middleware::Next,
};
use actix_web_httpauth::extractors::basic::BasicAuth;
use ulid::Ulid;

use crate::{
    audit::{ActorLog, AuditLogBuilder, RequestLog},
    handlers::{KINESIS_COMMON_ATTRIBUTES_KEY, STREAM_NAME_HEADER_KEY},
    rbac::{map::SessionKey, Users},
};

const DROP_HEADERS: [&str; 4] = ["authorization", "cookie", "user-agent", "x-p-stream"];

pub async fn audit_log_middleware(
    mut req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, actix_web::Error> {
    // Ensures that log will be pushed to subscriber on drop
    let mut log_builder = AuditLogBuilder::default();

    if let Some(kinesis_common_attributes) =
        req.request().headers().get(KINESIS_COMMON_ATTRIBUTES_KEY)
    {
        let attribute_value: &str = kinesis_common_attributes.to_str().unwrap();
        let message: Message = serde_json::from_str(attribute_value).unwrap();
        log_builder.set_stream_name(message.common_attributes.x_p_stream);
    } else if let Some(stream) = req.match_info().get("logstream") {
        log_builder.set_stream_name(stream.to_owned());
    } else if let Some(value) = req.headers().get(STREAM_NAME_HEADER_KEY) {
        if let Ok(stream) = value.to_str() {
            log_builder.set_stream_name(stream.to_owned());
        }
    }
    let mut username = "Unknown".to_owned();
    let mut authorization_method = "None".to_owned();

    // Extract authorization details from request, either from basic auth
    // header or cookie, else use default value.
    if let Ok(creds) = req.extract::<BasicAuth>().into_inner() {
        username = creds.user_id().trim().to_owned();
        authorization_method = "Basic Auth".to_owned();
    } else if let Some(cookie) = req.cookie("session") {
        authorization_method = "Session Cookie".to_owned();
        if let Some(user_id) = Ulid::from_string(cookie.value())
            .ok()
            .and_then(|ulid| Users.get_username_from_session(&SessionKey::SessionId(ulid)))
        {
            username = user_id;
        }
    }

    log_builder.request = RequestLog {
        method: req.method().to_string(),
        path: req.path().to_string(),
        protocol: req.connection_info().scheme().to_owned(),
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
    log_builder.actor = ActorLog {
        remote_host: req
            .connection_info()
            .realip_remote_addr()
            .unwrap_or_default()
            .to_owned(),
        user_agent: req
            .headers()
            .get("User-Agent")
            .and_then(|a| a.to_str().ok())
            .unwrap_or_default()
            .to_owned(),
        username,
        authorization_method,
    };
    log_builder.set_deployment_id().await;

    let res = next.call(req).await;

    // Capture status_code and error information from response
    match &res {
        Ok(res) => {
            let status = res.status();
            log_builder.response.status_code = status.as_u16();
            // Use error information from reponse object if an error
            if let Some(err) = res.response().error() {
                log_builder.set_response_error(err.to_string());
            }
        }
        Err(err) => log_builder.set_response_error(err.to_string()),
    }

    res
}
