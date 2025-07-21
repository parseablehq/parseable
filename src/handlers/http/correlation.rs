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

use actix_web::web::{Json, Path};
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use anyhow::Error;
use itertools::Itertools;

use crate::rbac::Users;
use crate::utils::actix::extract_session_key_from_req;
use crate::utils::{get_hash, get_user_from_request, user_auth_for_datasets};

use crate::correlation::{CORRELATIONS, CorrelationConfig, CorrelationError};

pub async fn list(req: HttpRequest) -> Result<impl Responder, CorrelationError> {
    let session_key = extract_session_key_from_req(&req)
        .map_err(|err| CorrelationError::AnyhowError(Error::msg(err.to_string())))?;

    let correlations = CORRELATIONS.list_correlations(&session_key).await?;

    Ok(web::Json(correlations))
}

pub async fn get(
    req: HttpRequest,
    correlation_id: Path<String>,
) -> Result<impl Responder, CorrelationError> {
    let correlation_id = correlation_id.into_inner();
    let session_key = extract_session_key_from_req(&req)
        .map_err(|err| CorrelationError::AnyhowError(Error::msg(err.to_string())))?;

    let correlation = CORRELATIONS.get_correlation(&correlation_id).await?;

    let permissions = Users.get_permissions(&session_key);

    let tables = &correlation
        .table_configs
        .iter()
        .map(|t| t.table_name.clone())
        .collect_vec();

    user_auth_for_datasets(&permissions, tables).await?;

    Ok(web::Json(correlation))
}

pub async fn post(
    req: HttpRequest,
    Json(mut correlation): Json<CorrelationConfig>,
) -> Result<impl Responder, CorrelationError> {
    let session_key = extract_session_key_from_req(&req)
        .map_err(|err| CorrelationError::AnyhowError(anyhow::Error::msg(err.to_string())))?;
    let user_id = get_user_from_request(&req)
        .map(|s| get_hash(&s.to_string()))
        .map_err(|err| CorrelationError::AnyhowError(Error::msg(err.to_string())))?;
    correlation.user_id = user_id;

    let correlation = CORRELATIONS.create(correlation, &session_key).await?;

    Ok(web::Json(correlation))
}

pub async fn modify(
    req: HttpRequest,
    correlation_id: Path<String>,
    Json(mut correlation): Json<CorrelationConfig>,
) -> Result<impl Responder, CorrelationError> {
    correlation.id = correlation_id.into_inner();
    correlation.user_id = get_user_from_request(&req)
        .map(|s| get_hash(&s.to_string()))
        .map_err(|err| CorrelationError::AnyhowError(Error::msg(err.to_string())))?;

    let session_key = extract_session_key_from_req(&req)
        .map_err(|err| CorrelationError::AnyhowError(anyhow::Error::msg(err.to_string())))?;

    let correlation = CORRELATIONS.update(correlation, &session_key).await?;

    Ok(web::Json(correlation))
}

pub async fn delete(
    req: HttpRequest,
    correlation_id: Path<String>,
) -> Result<impl Responder, CorrelationError> {
    let correlation_id = correlation_id.into_inner();
    let user_id = get_user_from_request(&req)
        .map(|s| get_hash(&s.to_string()))
        .map_err(|err| CorrelationError::AnyhowError(Error::msg(err.to_string())))?;

    CORRELATIONS.delete(&correlation_id, &user_id).await?;

    Ok(HttpResponse::Ok().finish())
}
