use std::sync::RwLock;

use actix_web::{http::header::ContentType, web, HttpRequest, HttpResponse, Responder};
use http::StatusCode;

use crate::{
    external_service::{DeRegistration, ModuleRegistry, Registration},
    option::CONFIG,
    storage::{self, ObjectStorageError, StorageMetadata},
};

pub async fn register(
    registration: web::Json<Registration>,
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<impl Responder, ModuleError> {
    let mut registration = registration.into_inner();
    let module_version = registration.version.clone();
    let version_result = registration.set_version(&module_version);

    if let Err(err) = version_result {
        return Err(ModuleError::VersionMismatch(err.to_string()));
    }

    let mut metadata = get_metadata().await?;
    metadata
        .modules
        .insert(registration.id.clone(), registration.clone());
    put_metadata(&metadata).await?;

    let module_id = registration.id.clone();
    registry.write().unwrap().register(registration);

    Ok(HttpResponse::Ok().body(format!("Added {}", module_id)))
}

pub async fn deregister(
    de_registration: web::Json<DeRegistration>,
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<impl Responder, ModuleError> {
    let de_registration = de_registration.into_inner();
    let module_id = de_registration.id.clone();
    let mut metadata = get_metadata().await?;

    metadata
        .modules
        .remove(&module_id)
        .ok_or_else(|| ModuleError::ModuleNotFound(module_id.to_owned()))?;
    put_metadata(&metadata).await?;

    registry.write().unwrap().deregister(&module_id);

    Ok(HttpResponse::Ok().body(format!("Deregistered {}", module_id)))
}

pub async fn get(
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<impl Responder, ModuleError> {
    let list: Vec<String> = registry
        .read()
        .unwrap()
        .registrations()
        .map(|x| x.id.clone())
        .collect();

    Ok(web::Json(list))
}

pub async fn get_config(path: web::Path<(String, String)>) -> Result<impl Responder, ModuleError> {
    let (name, _stream_name) = path.into_inner();
    let mut metadata = get_metadata().await?;
    let body = metadata
        .modules
        .remove(&name)
        .ok_or_else(|| ModuleError::ModuleNotFound(name.to_owned()))?;

    Ok(web::Json(body))
}

pub async fn put_config(
    path: web::Path<(String, String)>,
    config: web::Json<serde_json::Value>,
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<impl Responder, ModuleError> {
    let (name, stream_name) = path.into_inner();
    let config = config.into_inner();

    let (url, path_segment) = registry
        .read()
        .unwrap()
        .registrations()
        .find(|&registration| registration.id == name)
        .map(|registration| {
            (
                registration.url.clone(),
                registration.stream_config.path.clone(),
            )
        })
        .ok_or_else(|| ModuleError::ModuleNotFound(name.clone()))?;

    let path = path_segment.replace("{stream_name}", &stream_name);
    let url = url.join(&path)?;
    let client = reqwest::Client::new();
    let req_body = serde_json::to_vec(&config).expect("valid json");
    let request = client.post(url).body(req_body).build().unwrap();
    let resp = client.execute(request).await?;

    if !resp.status().is_success() {
        let resp_body = String::from_utf8_lossy(&resp.bytes().await?).to_string();
        return Err(ModuleError::Custom(resp_body.into()));
    }

    CONFIG
        .storage()
        .get_object_store()
        .put_module_config(&stream_name, name, config)
        .await?;
    Ok(HttpResponse::Ok())
}

pub async fn router(
    req: HttpRequest,
    params: web::Path<(String, String)>,
    body: web::Bytes,
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<HttpResponse, ModuleError> {
    let (name, path) = params.into_inner();
    let method: &http::Method = req.method();

    let (url, username, password) = {
        let module_registry = registry.read().unwrap();
        let registration = module_registry
            .get(&name)
            .ok_or_else(|| ModuleError::ModuleNotFound(name.clone()))?;

        if !registration.contains_route(&path, method) {
            return Ok(HttpResponse::NotFound().finish());
        }

        (
            registration.url.join(&path).expect("valid sub path"),
            registration.username.clone(),
            registration.password.clone(),
        )
    };

    let headers = req
        .headers()
        .iter()
        .filter(|(name, value)| {
            !value.is_sensitive() && !name.as_str().eq_ignore_ascii_case("authorization")
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();

    let client = reqwest::Client::new();
    let request = client
        .request(method.clone(), url)
        .headers(headers)
        .body(body)
        .basic_auth(username, Some(password));

    let request = request.build().unwrap();
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
pub enum ModuleError {
    #[error("Could not find module {0}")]
    ModuleNotFound(String),
    #[error("Object Store: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("Could not connect to module: {0}")]
    ModuleConnectionError(#[from] reqwest::Error),
    #[error("Serde json error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Bad Url: {0}")]
    BadUrl(#[from] url::ParseError),
    #[error("Module retuned an error: {0}")]
    Custom(#[from] Box<dyn std::error::Error>),
    #[error("Version mismatch: {0}")]
    VersionMismatch(String),
}

impl actix_web::ResponseError for ModuleError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            ModuleError::ModuleNotFound(_) => StatusCode::BAD_REQUEST,
            ModuleError::VersionMismatch(_) => StatusCode::BAD_REQUEST,
            ModuleError::ObjectStorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ModuleError::ModuleConnectionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ModuleError::Serde(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ModuleError::Custom(_) => StatusCode::BAD_REQUEST,
            ModuleError::BadUrl(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
