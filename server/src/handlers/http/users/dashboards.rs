use crate::{
    handlers::http::ingest::PostError,
    option::CONFIG,
    storage::{object_storage::dashboard_path, ObjectStorageError},
    users::dashboards::{Dashboard, DASHBOARDS},
};
use actix_web::{http::header::ContentType, web, HttpRequest, HttpResponse, Responder};
use bytes::Bytes;
use http::StatusCode;
use serde_json::{Error as SerdeError, Value as JsonValue};

pub async fn list(req: HttpRequest) -> Result<impl Responder, DashboardError> {
    let user_id = req
        .match_info()
        .get("user_id")
        .ok_or(DashboardError::Metadata("No User Id Provided"))?;

    // .users/user_id/dashboards/
    let path = dashboard_path(user_id, "");

    let store = CONFIG.storage().get_object_store();
    let dashboards = store
        .get_objects(
            Some(&path),
            Box::new(|file_name: String| file_name.ends_with("json")),
        )
        .await?;

    let mut dash = vec![];
    for dashboard in dashboards {
        dash.push(serde_json::from_slice::<JsonValue>(&dashboard)?)
    }

    Ok((web::Json(dash), StatusCode::OK))
}

pub async fn get(req: HttpRequest) -> Result<impl Responder, DashboardError> {
    let user_id = req
        .match_info()
        .get("user_id")
        .ok_or(DashboardError::Metadata("No User Id Provided"))?;

    let dash_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;

    if let Some(dashboard) = DASHBOARDS.find(dash_id) {
        return Ok((web::Json(dashboard), StatusCode::OK));
    }

    //if dashboard is not in memory fetch from s3
    let dash_file_path = dashboard_path(user_id, &format!("{}.json", dash_id));
    let resource = CONFIG
        .storage()
        .get_object_store()
        .get_object(&dash_file_path)
        .await?;
    let resource = serde_json::from_slice::<Dashboard>(&resource)?;

    Ok((web::Json(resource), StatusCode::OK))
}

pub async fn post(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    let user_id = req
        .match_info()
        .get("user_id")
        .ok_or(DashboardError::Metadata("No User Id Provided"))?;

    let dash_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;

    let dash_file_path = dashboard_path(user_id, &format!("{}.json", dash_id));

    let dashboard = serde_json::from_slice::<Dashboard>(&body)?;
    DASHBOARDS.update(dashboard);

    let store = CONFIG.storage().get_object_store();
    store.put_object(&dash_file_path, body).await?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, PostError> {
    let user_id = req
        .match_info()
        .get("user_id")
        .ok_or(DashboardError::Metadata("No User Id Provided"))?;

    let dash_id = req
        .match_info()
        .get("dashboard_id")
        .ok_or(DashboardError::Metadata("No Dashboard Id Provided"))?;

    let dash_file_path = dashboard_path(user_id, &format!("{}.json", dash_id));

    let store = CONFIG.storage().get_object_store();
    store.delete_object(&dash_file_path).await?;

    Ok(HttpResponse::Ok().finish())
}

#[derive(Debug, thiserror::Error)]
pub enum DashboardError {
    #[error("Failed to connect to storage: {0}")]
    ObjectStorage(#[from] ObjectStorageError),
    #[error("Serde Error: {0}")]
    Serde(#[from] SerdeError),
    #[error("Cannot perform this operation: {0}")]
    Metadata(&'static str),
}

impl actix_web::ResponseError for DashboardError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::ObjectStorage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serde(_) => StatusCode::BAD_REQUEST,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
