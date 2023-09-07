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

use actix_web::{http::header::ContentType, web, HttpResponse, Result};
use http::{header, StatusCode};
use reqwest;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::option::CONFIG;

const OPEN_AI_URL: &str = "https://api.openai.com/v1/chat/completions";

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    role: String,
    content: String,
}

#[derive(Deserialize, Debug)]
struct ResponseData {
    choices: Vec<Choice>,
}

#[derive(Deserialize, Debug)]
struct Choice {
    message: Message,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AiPrompt {
    prompt: String,
}

pub async fn make_llm_request(body: web::Json<AiPrompt>) -> Result<HttpResponse, LLMError> {
    let api_key = match &CONFIG.parseable.open_ai_key {
        Some(api_key) if api_key.len() > 3 => api_key,
        _ => return Err(LLMError::InvalidAPIKey),
    };

    let json_data = json!({
        "model": "gpt-3.5-turbo",
        "messages": [{ "role": "user", "content": body.prompt}],
        "temperature": 0.7,
    });

    let client = reqwest::Client::new();
    let response = client
        .post(OPEN_AI_URL)
        .header(header::CONTENT_TYPE, "application/json")
        .bearer_auth(api_key)
        .json(&json_data)
        .send()
        .await?;

    if response.status().is_success() {
        let body: ResponseData = response
            .json()
            .await
            .expect("OpenAI response is always the same");
        Ok(HttpResponse::Ok()
            .content_type("application/json")
            .json(&body.choices[0].message.content))
    } else {
        let body: Value = response.json().await?;
        let message = body
            .as_object()
            .and_then(|body| body.get("error"))
            .and_then(|error| error.as_object())
            .and_then(|error| error.get("message"))
            .map(|message| message.to_string())
            .unwrap_or_else(|| "Error from OpenAI".to_string());

        Err(LLMError::APIError(message))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LLMError {
    #[error("Either OpenAI key was not provided or was invalid")]
    InvalidAPIKey,
    #[error("Failed to call OpenAI endpoint: {0}")]
    FailedRequest(#[from] reqwest::Error),
    #[error("{0}")]
    APIError(String),
}

impl actix_web::ResponseError for LLMError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::InvalidAPIKey => StatusCode::INTERNAL_SERVER_ERROR,
            Self::FailedRequest(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::APIError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
