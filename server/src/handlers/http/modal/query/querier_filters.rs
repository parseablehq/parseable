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

use actix_web::{web, HttpRequest, HttpResponse, Responder};
use bytes::Bytes;
use chrono::Utc;
use http::StatusCode;

use crate::{
    handlers::http::{cluster::sync_with_queriers, modal::LEADER, users::filters::FiltersError},
    option::CONFIG,
    storage::object_storage::filter_path,
    users::filters::{Filter, CURRENT_FILTER_VERSION, FILTERS},
    utils::{get_hash, get_user_from_request},
};

use super::{LeaderRequest, Method};

pub async fn post(req: HttpRequest, body: Bytes) -> Result<impl Responder, FiltersError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);

    if LEADER.lock().await.is_leader() {
        let mut filter: Filter = serde_json::from_slice(&body)?;
        let filter_id = get_hash(Utc::now().timestamp_micros().to_string().as_str());
        filter.filter_id = Some(filter_id.clone());
        filter.user_id = Some(user_id.clone());
        filter.version = Some(CURRENT_FILTER_VERSION.to_string());
        FILTERS.update(&filter);

        let path = filter_path(
            &user_id,
            &filter.stream_name,
            &format!("{}.json", filter_id),
        );

        let store = CONFIG.storage().get_object_store();
        let filter_bytes = serde_json::to_vec(&filter)?;
        store.put_object(&path, Bytes::from(filter_bytes)).await?;

        sync_with_queriers(
            req.headers().clone(),
            Some(body),
            "filters/sync",
            Method::Post,
        )
        .await?;
        Ok((web::Json(filter), StatusCode::OK))
    } else {
        let request = LeaderRequest {
            body: Some(body),
            api: "filters",
            resource: None,
            method: Method::Post,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok((web::Json(res.json().await?), StatusCode::OK)),
            _ => {
                let err_msg = res.text().await?;
                Err(FiltersError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

pub async fn post_sync(req: HttpRequest, body: Bytes) -> Result<impl Responder, FiltersError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);

    let mut filter: Filter = serde_json::from_slice(&body)?;
    let filter_id = get_hash(Utc::now().timestamp_micros().to_string().as_str());
    filter.filter_id = Some(filter_id.clone());
    filter.user_id = Some(user_id.clone());
    filter.version = Some(CURRENT_FILTER_VERSION.to_string());
    FILTERS.update(&filter);

    filter_path(
        &user_id,
        &filter.stream_name,
        &format!("{}.json", filter_id),
    );

    Ok((web::Json(filter), StatusCode::OK))
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, FiltersError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let filter_id = req
        .match_info()
        .get("filter_id")
        .ok_or(FiltersError::Metadata("No Filter Id Provided"))?;

    if LEADER.lock().await.is_leader() {
        let filter = FILTERS
            .get_filter(filter_id, &user_id)
            .ok_or(FiltersError::Metadata("Filter does not exist"))?;

        FILTERS.delete_filter(filter_id);

        let path = filter_path(
            &user_id,
            &filter.stream_name,
            &format!("{}.json", filter_id),
        );
        let store = CONFIG.storage().get_object_store();
        store.delete_object(&path).await?;

        sync_with_queriers(
            req.headers().clone(),
            None,
            &format!("filters/{filter_id}/sync"),
            Method::Delete,
        )
        .await?;
        Ok(HttpResponse::Ok().finish())
    } else {
        let resource = filter_id.to_string();
        let request = LeaderRequest {
            body: None,
            api: "filters",
            resource: Some(&resource),
            method: Method::Delete,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok(HttpResponse::Ok().finish()),
            _ => {
                let err_msg = res.text().await?;
                Err(FiltersError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

pub async fn delete_sync(req: HttpRequest) -> Result<HttpResponse, FiltersError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let filter_id = req
        .match_info()
        .get("filter_id")
        .ok_or(FiltersError::Metadata("No Filter Id Provided"))?;

    FILTERS
        .get_filter(filter_id, &user_id)
        .ok_or(FiltersError::Metadata("Filter does not exist"))?;

    FILTERS.delete_filter(filter_id);

    Ok(HttpResponse::Ok().finish())
}

pub async fn update(req: HttpRequest, body: Bytes) -> Result<impl Responder, FiltersError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let filter_id = req
        .match_info()
        .get("filter_id")
        .ok_or(FiltersError::Metadata("No Filter Id Provided"))?;

    if LEADER.lock().await.is_leader() {
        if FILTERS.get_filter(filter_id, &user_id).is_none() {
            return Err(FiltersError::Metadata("Filter does not exist"));
        }
        let mut filter: Filter = serde_json::from_slice(&body)?;
        filter.filter_id = Some(filter_id.to_string());
        filter.user_id = Some(user_id.clone());
        filter.version = Some(CURRENT_FILTER_VERSION.to_string());
        FILTERS.update(&filter);

        let path = filter_path(
            &user_id,
            &filter.stream_name,
            &format!("{}.json", filter_id),
        );

        let store = CONFIG.storage().get_object_store();
        let filter_bytes = serde_json::to_vec(&filter)?;
        store.put_object(&path, Bytes::from(filter_bytes)).await?;

        sync_with_queriers(
            req.headers().clone(),
            Some(body),
            &format!("filters/{filter_id}/sync"),
            Method::Put,
        )
        .await?;
        Ok((web::Json(filter), StatusCode::OK))
    } else {
        let resource = filter_id.to_string();
        let request = LeaderRequest {
            body: None,
            api: "filters",
            resource: Some(&resource),
            method: Method::Put,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok((web::Json(res.json().await?), StatusCode::OK)),
            _ => {
                let err_msg = res.text().await?;
                Err(FiltersError::Anyhow(anyhow::Error::msg(err_msg)))
            }
        }
    }
}

pub async fn update_sync(req: HttpRequest, body: Bytes) -> Result<impl Responder, FiltersError> {
    let mut user_id = get_user_from_request(&req)?;
    user_id = get_hash(&user_id);
    let filter_id = req
        .match_info()
        .get("filter_id")
        .ok_or(FiltersError::Metadata("No Filter Id Provided"))?;

    if FILTERS.get_filter(filter_id, &user_id).is_none() {
        return Err(FiltersError::Metadata("Filter does not exist"));
    }
    let mut filter: Filter = serde_json::from_slice(&body)?;
    filter.filter_id = Some(filter_id.to_string());
    filter.user_id = Some(user_id.clone());
    filter.version = Some(CURRENT_FILTER_VERSION.to_string());
    FILTERS.update(&filter);

    Ok((web::Json(filter), StatusCode::OK))
}
