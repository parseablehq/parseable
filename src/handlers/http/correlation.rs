/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
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

use actix_web::web::Path;
use actix_web::{web, HttpRequest, HttpResponse, Responder};
use anyhow::Error;
use bytes::Bytes;
use itertools::Itertools;

use crate::rbac::Users;
use crate::utils::{get_hash, get_user_from_request, user_auth_for_query};
use crate::{option::CONFIG, utils::actix::extract_session_key_from_req};

use crate::correlation::{CorrelationConfig, CorrelationError, CorrelationRequest, CORRELATIONS};

pub async fn list(req: HttpRequest) -> Result<impl Responder, CorrelationError> {
    let session_key = extract_session_key_from_req(&req)
        .map_err(|err| CorrelationError::AnyhowError(Error::msg(err.to_string())))?;

    let user_id = get_user_from_request(&req)
        .map(|s| get_hash(&s.to_string()))
        .map_err(|err| CorrelationError::AnyhowError(Error::msg(err.to_string())))?;

    let correlations = CORRELATIONS
        .list_correlations_for_user(&session_key, &user_id)
        .await?;

    Ok(web::Json(correlations))
}

pub async fn get(req: HttpRequest) -> Result<impl Responder, CorrelationError> {
    let session_key = extract_session_key_from_req(&req)
        .map_err(|err| CorrelationError::AnyhowError(Error::msg(err.to_string())))?;

    let user_id = get_user_from_request(&req)
        .map(|s| get_hash(&s.to_string()))
        .map_err(|err| CorrelationError::AnyhowError(Error::msg(err.to_string())))?;

    let correlation_id = req
        .match_info()
        .get("correlation_id")
        .ok_or(CorrelationError::Metadata("No correlation ID Provided"))?;

    let correlation = CORRELATIONS
        .get_correlation(correlation_id, &user_id)
        .await?;

    let permissions = Users.get_permissions(&session_key);

    let tables = &correlation
        .table_configs
        .iter()
        .map(|t| t.table_name.clone())
        .collect_vec();

    user_auth_for_query(&permissions, tables)?;

    Ok(web::Json(correlation))
}

pub async fn post(req: HttpRequest, body: Bytes) -> Result<impl Responder, CorrelationError> {
    let session_key = extract_session_key_from_req(&req)
        .map_err(|err| CorrelationError::AnyhowError(anyhow::Error::msg(err.to_string())))?;

    let correlation_request: CorrelationRequest = serde_json::from_slice(&body)?;

    correlation_request.validate(&session_key).await?;

    let correlation: CorrelationConfig = correlation_request.into();

    // Save to memory
    CORRELATIONS.update(&correlation).await?;

    Ok(web::Json(correlation))
}

pub async fn modify(req: HttpRequest, body: Bytes) -> Result<impl Responder, CorrelationError> {
    let session_key = extract_session_key_from_req(&req)
        .map_err(|err| CorrelationError::AnyhowError(anyhow::Error::msg(err.to_string())))?;
    let user_id = get_user_from_request(&req)
        .map(|s| get_hash(&s.to_string()))
        .map_err(|err| CorrelationError::AnyhowError(Error::msg(err.to_string())))?;

    let correlation_id = req
        .match_info()
        .get("correlation_id")
        .ok_or(CorrelationError::Metadata("No correlation ID Provided"))?;

    // validate whether user has access to this correlation object or not
    let correlation = CORRELATIONS
        .get_correlation(correlation_id, &user_id)
        .await?;
    let permissions = Users.get_permissions(&session_key);
    let tables = &correlation
        .table_configs
        .iter()
        .map(|t| t.table_name.clone())
        .collect_vec();

    user_auth_for_query(&permissions, tables)?;

    let correlation_request: CorrelationRequest = serde_json::from_slice(&body)?;
    correlation_request.validate(&session_key).await?;

    let correlation =
        correlation_request.generate_correlation_config(correlation_id.to_owned(), user_id.clone());

    let path = correlation.path();

    let store = CONFIG.storage().get_object_store();
    let correlation_bytes = serde_json::to_vec(&correlation)?;
    store
        .put_object(&path, Bytes::from(correlation_bytes))
        .await?;

    // Save to memory
    CORRELATIONS.update(&correlation).await?;

    Ok(web::Json(correlation))
}

pub async fn delete(
    req: HttpRequest,
    correlation_id: Path<String>,
) -> Result<impl Responder, CorrelationError> {
    let correlation_id = correlation_id.into_inner();
    let session_key = extract_session_key_from_req(&req)
        .map_err(|err| CorrelationError::AnyhowError(anyhow::Error::msg(err.to_string())))?;
    let user_id = get_user_from_request(&req)
        .map(|s| get_hash(&s.to_string()))
        .map_err(|err| CorrelationError::AnyhowError(Error::msg(err.to_string())))?;

    let correlation = CORRELATIONS
        .get_correlation(&correlation_id, &user_id)
        .await?;

    // validate user's query auth
    let permissions = Users.get_permissions(&session_key);
    let tables = &correlation
        .table_configs
        .iter()
        .map(|t| t.table_name.clone())
        .collect_vec();

    user_auth_for_query(&permissions, tables)?;

    CORRELATIONS.delete(&correlation).await?;

    Ok(HttpResponse::Ok().finish())
}
