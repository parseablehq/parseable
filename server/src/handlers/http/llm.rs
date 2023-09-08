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

use crate::{metadata::StreamInfo, option::CONFIG};
use once_cell::sync::Lazy;

const OPEN_AI_URL: &str = "https://api.openai.com/v1/chat/completions";
pub static STREAM_INFO: Lazy<StreamInfo> = Lazy::new(StreamInfo::default);

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
    stream: String,
    stringified: String,
}

#[derive(Debug)]
struct Field {
    name: String,
    data_type: String,
}

fn filter_unnecessary_fields_from_record(obj_array: Vec<Field>) -> Vec<Field> {
    let mut filtered_fields: Vec<Field> = Vec::new();
    for obj in obj_array {
        let name = obj.name.clone();
        let data_type = obj.data_type.clone();
        let filtered_field = Field { name, data_type };

        filtered_fields.push(filtered_field);
    }

    return filtered_fields;
}

pub async fn make_llm_request(body: web::Json<AiPrompt>) -> Result<HttpResponse, LLMError> {
    let api_key = match &CONFIG.parseable.open_ai_key {
        Some(api_key) if api_key.len() > 3 => api_key,
        _ => return Err(LLMError::InvalidAPIKey),
    };

    let stream_name: String = body.stream;

    let schema = STREAM_INFO.schema(&stream_name)?;
    println!("{:?}", schema);

    let filtered_schema = filter_unnecessary_fields_from_record(schema);

    let schema_json = serde_json::to_string(&filtered_schema).expect("");
    println!("{:?}", schema_json);

    let ai_prompt = format!("I have a table called {}. It has the columns:\n{}\nBased on this, generate valid SQL for the query: \"{}\"\nGenerate only SQL as output. Also add comments in SQL syntax to explain your actions. Don't output anything else.\nIf it is not possible to generate valid SQL, output an SQL comment saying so.",
                     body.stream, schema_json, body.prompt);

    let json_data = json!({
        "model": "gpt-3.5-turbo",
        "messages": [{ "role": "user", "content": ai_prompt}],
        "temperature": 0.6,
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
