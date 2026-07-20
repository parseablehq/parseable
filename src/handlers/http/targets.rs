/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use actix_web::{
    HttpRequest, Responder,
    web::{self, Json, Path},
};
use futures::stream::FuturesUnordered;
use serde_json::json;
use tracing::error;
use ulid::Ulid;

use crate::{
    alerts::{
        AlertError,
        target::{TARGETS, Target},
    },
    utils::get_tenant_id_from_request,
};

// POST /targets
pub async fn post(
    req: HttpRequest,
    Json(mut target): Json<Target>,
) -> Result<impl Responder, AlertError> {
    let tenant_id = get_tenant_id_from_request(&req);
    target.tenant = tenant_id;
    target.validate_outbound_policy().await?;
    // should check for duplicacy and liveness (??)
    // add to the map
    TARGETS.update(target.clone()).await?;

    Ok(web::Json(target.mask()))
}

// GET /targets
pub async fn list(req: HttpRequest) -> Result<impl Responder, AlertError> {
    let tenant_id = get_tenant_id_from_request(&req);
    let handles = FuturesUnordered::new();
    // add to the map
    let mut list = vec![];
    for target in TARGETS.list(&tenant_id).await? {
        handles.push(tokio::spawn(async move {
            if let Err(err) = target.validate_outbound_policy().await {
                error!(error = %err, "Alert target rejected during outbound policy validation");
                let message = match &err {
                    AlertError::OutboundPolicy(policy_error) => policy_error.sanitized_message(),
                    _ => "Target validation failed",
                };
                json!({
                    "target": &target.mask(),
                    "enabled": false,
                    "error": message
                })
            } else {
                json!({
                    "target": &target.mask(),
                    "enabled": true
                })
            }
        }));
    }
    for res in handles {
        list.push(
            res.await
                .map_err(|e| AlertError::CustomError(e.to_string()))?,
        );
    }

    Ok(web::Json(list))
}

// GET /targets/{target_id}
pub async fn get(req: HttpRequest, target_id: Path<Ulid>) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let target = TARGETS.get_target_by_id(&target_id, &tenant_id).await?;
    let res = if let Err(e) = target.validate_outbound_policy().await {
        let message = match &e {
            AlertError::OutboundPolicy(policy_error) => policy_error.sanitized_message(),
            _ => "Target validation failed",
        };
        json!({
            "target": &target.mask(),
            "enabled": false,
            "error": message
        })
    } else {
        json!({
            "target": &target.mask(),
            "enabled": true
        })
    };
    Ok(web::Json(res))
}

// PUT /targets/{target_id}
pub async fn update(
    req: HttpRequest,
    target_id: Path<Ulid>,
    Json(mut target): Json<Target>,
) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    // if target_id does not exist, error
    let old_target = TARGETS.get_target_by_id(&target_id, &tenant_id).await?;

    // do not allow modifying name
    if old_target.name != target.name {
        return Err(AlertError::InvalidTargetModification(
            "Can't modify target name".to_string(),
        ));
    }

    // esnure that the supplied target id is assigned to the target config
    target.id = target_id;
    target.tenant = tenant_id;
    target.validate_outbound_policy().await?;
    // should check for duplicacy and liveness (??)
    // add to the map
    TARGETS.update(target.clone()).await?;

    Ok(web::Json(target.mask()))
}

// DELETE /targets/{target_id}
pub async fn delete(req: HttpRequest, target_id: Path<Ulid>) -> Result<impl Responder, AlertError> {
    let target_id = target_id.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let target = TARGETS.delete(&target_id, &tenant_id).await?;

    Ok(web::Json(target.mask()))
}

#[cfg(test)]
mod tests {
    use actix_web::{App, test};
    use serde_json::{Value, json};

    use super::*;
    use crate::alerts::{
        outbound_http_policy::{ALERT_TARGET_POLICY, AlertTargetPolicyConfig},
        target::Target,
    };

    #[actix_web::test]
    async fn list_hides_outbound_policy_error_details() {
        let tenant = format!("targets-handler-test-{}", Ulid::new());
        let denied_domain = "sensitive.internal.example";
        let target: Target = serde_json::from_value(json!({
            "name": "sensitive-target",
            "type": "webhook",
            "endpoint": format!("https://{denied_domain}/hook"),
            "tenantId": tenant
        }))
        .unwrap();

        TARGETS
            .target_configs
            .write()
            .await
            .entry(tenant.clone())
            .or_default()
            .insert(target.id, target);
        ALERT_TARGET_POLICY.write().await.insert(
            tenant.clone(),
            AlertTargetPolicyConfig {
                denied_domains: vec![denied_domain.to_string()],
                ..Default::default()
            },
        );

        let app = test::init_service(App::new().route("/targets", web::get().to(list))).await;
        let request = test::TestRequest::get()
            .uri("/targets")
            .insert_header(("x-p-tenant", tenant.clone()))
            .to_request();
        let response: Value = test::call_and_read_body_json(&app, request).await;

        TARGETS.target_configs.write().await.remove(&tenant);
        ALERT_TARGET_POLICY.write().await.remove(&tenant);

        assert_eq!(
            response[0]["error"],
            "Target domain is denied by outbound policy"
        );
        assert!(!response.to_string().contains(denied_domain));
    }
}
