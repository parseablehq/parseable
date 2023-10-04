use std::sync::RwLock;

use actix_web::{http::header::ContentType, web, HttpRequest, HttpResponse, Responder};
use http::StatusCode;

use crate::{
    external_service::{ModuleRegistry, Registration},
    option::CONFIG,
    storage::{self, ObjectStorageError, StorageMetadata},
};

pub async fn register(
    registration: web::Json<Registration>,
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<impl Responder, RegistrationError> {
    registry
        .write()
        .unwrap()
        .register(registration.into_inner());
    Ok(HttpResponse::Ok().finish())
}

pub async fn get(
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<impl Responder, RegistrationError> {
    let list: Vec<String> = registry
        .read()
        .unwrap()
        .registrations()
        .map(|x| x.id.clone())
        .collect();

    Ok(web::Json(list))
}

pub async fn get_config(
    path: web::Path<(String, String)>,
) -> Result<impl Responder, RegistrationError> {
    let (name, _stream_name) = path.into_inner();
    let mut metadata = get_metadata().await?;
    let body = metadata
        .modules
        .remove(&*name)
        .ok_or_else(|| RegistrationError::ModuleNotFound(name.to_owned()))?;

    Ok(web::Json(body))
}

pub async fn put_config(
    path: web::Path<(String, String)>,
    body: web::Json<serde_json::Value>,
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<impl Responder, RegistrationError> {
    let (name, _stream_name) = path.into_inner();
    let url = registry
        .read()
        .unwrap()
        .registrations()
        .find(|&registration| registration.id == name)
        .map(|registration| &registration.url)
        .cloned()
        .ok_or_else(|| RegistrationError::ModuleNotFound(name.clone()))?;

    let url = url.join("/config").expect("valid url");
    let client = reqwest::Client::new();
    let body = serde_json::to_vec(&body).expect("valid json");
    let request = client.post(url).body(body).build().unwrap();
    let resp = client.execute(request).await?;
    let resp_body = resp.bytes().await?;
    let resp_body = serde_json::from_slice(&resp_body)?;
    let mut metadata = get_metadata().await?;
    metadata.modules.insert(name, resp_body);
    put_metadata(&metadata).await?;

    Ok(HttpResponse::Ok())
}

pub async fn router(
    req: HttpRequest,
    params: web::Path<(String, String)>,
    body: web::Bytes,
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<HttpResponse, RegistrationError> {
    let (name, path) = params.into_inner();
    let method: &http::Method = req.method();

    let url = {
        let module_registry = &registry.read().unwrap();
        let registration = module_registry
            .get(&name)
            .ok_or_else(|| RegistrationError::ModuleNotFound(name.clone()))?;

        if !registration.contains_route(&path, method) {
            return Ok(HttpResponse::NotFound().finish());
        }

        registration.url.join(&path).expect("valid sub path")
    };

    let client = reqwest::Client::new();
    let request = client
        .request(method.clone(), url)
        .body(body)
        .build()
        .unwrap();
    let resp = client.execute(request).await?;
    Ok(HttpResponse::Ok().body(resp.bytes().await?))
}

async fn get_metadata() -> Result<crate::storage::StorageMetadata, ObjectStorageError> {
    let metadata = CONFIG
        .storage()
        .get_object_store()
        .get_metadata()
        .await?
        .expect("metadata is initialized");
    Ok(metadata)
}

async fn put_metadata(metadata: &StorageMetadata) -> Result<(), ObjectStorageError> {
    storage::put_remote_metadata(metadata).await?;
    storage::put_staging_metadata(metadata)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    #[error("Could not find module {0}")]
    ModuleNotFound(String),
    #[error("Object Store: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("Could not connect to module: {0}")]
    ModuleConnectionError(#[from] reqwest::Error),
    #[error("Serde json error: {0}")]
    Serde(#[from] serde_json::Error),
}

impl actix_web::ResponseError for RegistrationError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            RegistrationError::ModuleNotFound(_) => StatusCode::BAD_REQUEST,
            RegistrationError::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            RegistrationError::ModuleConnectionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            RegistrationError::Serde(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
