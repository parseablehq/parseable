/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

use actix_web::{HttpRequest, HttpResponse, web};
use base64::{Engine, prelude::BASE64_STANDARD};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::{
    INTRA_CLUSTER_CLIENT,
    handlers::{AUTHORIZATION_KEY, TENANT_ID},
    option::Mode,
    otel_generator::OTEL_GENERATOR,
    parseable::{DEFAULT_TENANT, PARSEABLE},
    utils::{get_tenant_id_from_request, get_user_from_request},
};

use super::{
    base_path_without_preceding_slash,
    cluster::{get_node_info, utils::check_liveness},
    middleware::{CLUSTER_SECRET, CLUSTER_SECRET_HEADER},
    modal::{NodeMetadata, NodeType},
};

const FORWARDED_AUTHORIZATION: &str = "x-p-otel-generator-authorization";
const MAX_DURATION_SECS: u64 = 7 * 24 * 60 * 60;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StartGeneratorRequest {
    #[serde(default = "default_duration")]
    duration_secs: u64,
}

fn default_duration() -> u64 {
    86_400
}

#[derive(Serialize)]
struct OtelGeneratorErrorResponse {
    error: String,
}

pub async fn start_otel_generator(
    req: HttpRequest,
    body: Option<web::Json<StartGeneratorRequest>>,
) -> HttpResponse {
    let tenant_id = match request_tenant(&req) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let duration_secs = body
        .as_ref()
        .map_or_else(default_duration, |body| body.duration_secs);
    if duration_secs == 0 || duration_secs > MAX_DURATION_SECS {
        return HttpResponse::BadRequest().json(OtelGeneratorErrorResponse {
            error: format!("durationSecs must be between 1 and {MAX_DURATION_SECS}"),
        });
    }

    match PARSEABLE.options.mode {
        Mode::Query | Mode::Prism => {
            let payload = serde_json::to_vec(&StartGeneratorRequest { duration_secs })
                .expect("generator request serializes");
            forward_to_ingestor(&req, Method::POST, Some(payload), tenant_id).await
        }
        Mode::Ingest | Mode::All => {
            let auth = match generator_authorization(&req) {
                Ok(auth) => auth,
                Err(response) => return response,
            };
            let endpoint = PARSEABLE
                .options
                .get_url(PARSEABLE.options.mode)
                .to_string()
                .trim_end_matches('/')
                .to_string();
            info!(%endpoint, duration_secs, "starting OTel demo generator");
            match OTEL_GENERATOR.start(&endpoint, &auth, Some(duration_secs), tenant_id.as_deref())
            {
                Ok(result) if result.status == "started" => {
                    HttpResponse::Accepted().json(serde_json::json!({
                        "status": "accepted",
                        "message": result.message,
                    }))
                }
                Ok(result) => HttpResponse::Conflict().json(OtelGeneratorErrorResponse {
                    error: result.message,
                }),
                Err(error) => {
                    error!(%error, "failed to start OTel demo generator");
                    HttpResponse::InternalServerError().json(OtelGeneratorErrorResponse {
                        error: format!("Failed to start generator: {error}"),
                    })
                }
            }
        }
        Mode::Index => unavailable_in_mode(),
    }
}

pub async fn stop_otel_generator(req: HttpRequest) -> HttpResponse {
    let tenant_id = match request_tenant(&req) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    match PARSEABLE.options.mode {
        Mode::Query | Mode::Prism => {
            forward_to_ingestor(&req, Method::DELETE, None, tenant_id).await
        }
        Mode::Ingest | Mode::All => {
            HttpResponse::Ok().json(OTEL_GENERATOR.stop(tenant_id.as_deref()))
        }
        Mode::Index => unavailable_in_mode(),
    }
}

pub async fn get_otel_generator_status(req: HttpRequest) -> HttpResponse {
    let tenant_id = match request_tenant(&req) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    match PARSEABLE.options.mode {
        Mode::Query | Mode::Prism => forward_to_ingestor(&req, Method::GET, None, tenant_id).await,
        Mode::Ingest | Mode::All => {
            HttpResponse::Ok().json(OTEL_GENERATOR.status(tenant_id.as_deref()))
        }
        Mode::Index => unavailable_in_mode(),
    }
}

fn request_tenant(req: &HttpRequest) -> Result<Option<String>, HttpResponse> {
    let tenant_id = get_tenant_id_from_request(req);
    if PARSEABLE.options.is_multi_tenant() && tenant_id.is_none() {
        return Err(HttpResponse::BadRequest().json(OtelGeneratorErrorResponse {
            error: "X-P-Tenant header is required when multi-tenancy is enabled".to_string(),
        }));
    }
    Ok(tenant_id)
}

async fn forward_to_ingestor(
    req: &HttpRequest,
    method: Method,
    body: Option<Vec<u8>>,
    tenant_id: Option<String>,
) -> HttpResponse {
    let mut ingestors: Vec<NodeMetadata> = match get_node_info(NodeType::Ingestor, &tenant_id).await
    {
        Ok(ingestors) => ingestors,
        Err(error) => {
            error!(%error, "failed to load ingestors for OTel generator request");
            return HttpResponse::ServiceUnavailable().json(OtelGeneratorErrorResponse {
                error: format!("Failed to find an ingestor: {error}"),
            });
        }
    };
    ingestors.sort_by(|left, right| left.domain_name.cmp(&right.domain_name));

    let mut selected = None;
    for ingestor in ingestors {
        if check_liveness(&ingestor.domain_name).await {
            selected = Some(ingestor);
            break;
        }
    }
    let Some(ingestor) = selected else {
        return HttpResponse::ServiceUnavailable().json(OtelGeneratorErrorResponse {
            error: "No live ingestors found".to_string(),
        });
    };

    let url = format!(
        "{}{}/otel_generator",
        ingestor.domain_name,
        base_path_without_preceding_slash()
    );
    let forwards_authorization = method == Method::POST;
    let mut request = INTRA_CLUSTER_CLIENT
        .request(method, url)
        .header(AUTHORIZATION_KEY, &ingestor.token)
        .header(reqwest::header::CONTENT_TYPE, "application/json");
    if let Some((_, hash)) = CLUSTER_SECRET.get() {
        let user = match get_user_from_request(req) {
            Ok(user) => user,
            Err(error) => {
                return HttpResponse::Unauthorized().json(OtelGeneratorErrorResponse {
                    error: format!("Failed to identify requesting user: {error}"),
                });
            }
        };
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        request = request
            .header(CLUSTER_SECRET_HEADER, hash)
            .header("intra-cluster-tenant", tenant)
            .header("intra-cluster-userid", user);
    } else if PARSEABLE.options.is_multi_tenant() {
        return HttpResponse::InternalServerError().json(OtelGeneratorErrorResponse {
            error: "P_CLUSTER_SECRET is required for distributed multi-tenancy".to_string(),
        });
    }
    if let Some(tenant_id) = tenant_id.as_deref() {
        request = request.header(TENANT_ID, tenant_id);
    }
    if forwards_authorization
        && let Some(auth) = req
            .headers()
            .get(AUTHORIZATION_KEY)
            .and_then(|value| value.to_str().ok())
    {
        request = request.header(FORWARDED_AUTHORIZATION, auth);
    }
    if let Some(body) = body {
        request = request.body(body);
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            error!(%error, ingestor = ingestor.domain_name, "failed to forward OTel generator request");
            return HttpResponse::BadGateway().json(OtelGeneratorErrorResponse {
                error: format!("Failed to forward request to ingestor: {error}"),
            });
        }
    };
    let status = actix_web::http::StatusCode::from_u16(response.status().as_u16())
        .unwrap_or(actix_web::http::StatusCode::BAD_GATEWAY);
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .cloned();
    let response_body = match response.bytes().await {
        Ok(body) => body,
        Err(error) => {
            return HttpResponse::BadGateway().json(OtelGeneratorErrorResponse {
                error: format!("Failed to read ingestor response: {error}"),
            });
        }
    };
    let mut forwarded = HttpResponse::build(status);
    if let Some(content_type) = content_type
        && let Ok(content_type) = content_type.to_str()
    {
        forwarded.insert_header((actix_web::http::header::CONTENT_TYPE, content_type));
    }
    forwarded.body(response_body)
}

fn generator_authorization(req: &HttpRequest) -> Result<String, HttpResponse> {
    let header = if PARSEABLE.options.mode == Mode::Ingest {
        FORWARDED_AUTHORIZATION
    } else {
        AUTHORIZATION_KEY
    };
    if let Some(auth) = req
        .headers()
        .get(header)
        .and_then(|value| value.to_str().ok())
    {
        return Ok(auth.to_owned());
    }

    if !PARSEABLE.options.is_multi_tenant() {
        return Ok(format!(
            "Basic {}",
            BASE64_STANDARD.encode(format!(
                "{}:{}",
                PARSEABLE.options.username, PARSEABLE.options.password
            ))
        ));
    }

    Err(HttpResponse::BadRequest().json(OtelGeneratorErrorResponse {
        error: "Authorization header is required to export generated telemetry".to_string(),
    }))
}

fn unavailable_in_mode() -> HttpResponse {
    HttpResponse::BadRequest().json(OtelGeneratorErrorResponse {
        error: format!(
            "OTel generator is not available in {:?} mode",
            PARSEABLE.options.mode
        ),
    })
}
