/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use std::sync::RwLock;

use actix_web::{http::header::ContentType, web, HttpRequest, HttpResponse, Responder};
use http::StatusCode;

use crate::{
    external_service::{ModuleRegistry, Registration},
    metadata::STREAM_INFO,
    option::CONFIG,
    storage::{self, ObjectStorageError, StorageMetadata},
};

pub async fn register(
    registration: web::Json<Registration>,
    module_name: web::Path<String>,
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<impl Responder, ModuleError> {
    let registration = registration.into_inner();
    validate_version(&registration.version)?;

    let module_name = module_name.into_inner();

    let mut metadata = get_metadata().await?;
    metadata
        .modules
        .insert(module_name.clone(), registration.clone());
    put_metadata(&metadata).await?;

    registry
        .write()
        .unwrap()
        .register(module_name.clone(), registration);

    Ok(HttpResponse::Ok().body(format!("Added {}", module_name)))
}

pub async fn deregister(
    module_name: web::Path<String>,
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<impl Responder, ModuleError> {
    let module_name = module_name.into_inner();
    let mut metadata = get_metadata().await?;

    metadata
        .modules
        .remove(&module_name)
        .ok_or_else(|| ModuleError::ModuleNotFound(module_name.to_owned()))?;
    put_metadata(&metadata).await?;

    registry.write().unwrap().deregister(&module_name);

    Ok(HttpResponse::Ok().body(format!("Deregistered {}", module_name)))
}

pub async fn list_modules(
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<impl Responder, ModuleError> {
    let list: Vec<String> = registry.read().unwrap().get_keys();
    Ok(web::Json(list))
}

pub async fn get_config(path: web::Path<(String, String)>) -> Result<impl Responder, ModuleError> {
    let (name, stream_name) = path.into_inner();

    if !STREAM_INFO.stream_exists(&stream_name) {
        return Err(ModuleError::StreamDoesNotExist(stream_name.clone()));
    }

    let mut metadata = CONFIG
        .storage()
        .get_object_store()
        .get_stream_metadata(&stream_name)
        .await?;

    let config = metadata
        .modules
        .remove(&name)
        .ok_or_else(|| ModuleError::ModuleNotFound(name.clone()))?;

    Ok(web::Json(config))
}

pub async fn put_config(
    path: web::Path<(String, String)>,
    config: web::Json<serde_json::Value>,
    registry: web::Data<RwLock<ModuleRegistry>>,
) -> Result<impl Responder, ModuleError> {
    let (name, stream_name) = path.into_inner();
    let config = config.into_inner();

    if !STREAM_INFO.stream_exists(&stream_name) {
        return Err(ModuleError::StreamDoesNotExist(stream_name.clone()));
    }

    let registration = registry
        .read()
        .unwrap()
        .get(&name)
        .cloned()
        .ok_or_else(|| ModuleError::ModuleNotFound(name.clone()))?;

    let path = registration
        .stream_config
        .path
        .replace("{stream_name}", &stream_name);

    let url = registration.url.join(&path)?;

    let client = reqwest::Client::new();
    let req_body = serde_json::to_vec(&config).expect("valid json");

    let request = client
        .put(url)
        .body(req_body)
        .basic_auth(registration.username, Some(registration.password))
        .header(reqwest::header::CONTENT_TYPE, "application/json") // Set the Content-Type header
        .build()
        .unwrap();
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

        let module_path = match registration.get_module_path(&path, method) {
            Some(path) => path,
            None => return Ok(HttpResponse::NotFound().finish()),
        };

        (
            registration.url.join(&module_path).expect("valid sub path"),
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

pub fn validate_version(version: &str) -> Result<(), ModuleError> {
    if let Some(semver) = version.strip_prefix('v') {
        semver::Version::parse(semver)
            .map(|_| ())
            .map_err(|_| ModuleError::VersionMismatch("Invalid SemVer format".to_string()))
    } else {
        Err(ModuleError::VersionMismatch(
            "Module version must start with 'v'".to_string(),
        ))
    }
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
    #[error("Stream {0} does not exist.")]
    StreamDoesNotExist(String),
}

impl actix_web::ResponseError for ModuleError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            ModuleError::ModuleNotFound(_) => StatusCode::BAD_REQUEST,
            ModuleError::VersionMismatch(_) => StatusCode::BAD_REQUEST,
            ModuleError::StreamDoesNotExist(_) => StatusCode::BAD_REQUEST,
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
