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

use actix_web::{HttpResponse, Result, http::header::ContentType, web};
use http::{StatusCode, header};
use itertools::Itertools;
use reqwest;
use serde_json::{Value, json};

use crate::{parseable::PARSEABLE, parseable::StreamNotFound};

const OPEN_AI_URL: &str = "https://api.openai.com/v1/chat/completions";

// Deserialize types for OpenAI Response
#[derive(serde::Deserialize, Debug)]
struct ResponseData {
    choices: Vec<Choice>,
}

#[derive(serde::Deserialize, Debug)]
struct Choice {
    message: Message,
}

#[derive(serde::Deserialize, Debug)]
struct Message {
    content: String,
}

// Request body
#[derive(serde::Deserialize, Debug)]
pub struct AiPrompt {
    prompt: String,
    stream: String,
}

// Temperory type
#[derive(Debug, serde::Serialize)]
pub struct Field {
    name: String,
    data_type: String,
}

impl From<&arrow_schema::Field> for Field {
    fn from(field: &arrow_schema::Field) -> Self {
        Self {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
        }
    }
}

fn build_prompt(stream: &str, prompt: &str, schema_json: &str) -> String {
    format!(
        r#"I have a table called {stream}.
It has the columns:\n{schema_json}
Based on this schema, generate valid SQL for the query: "{prompt}"
Generate only simple SQL as output. Also add comments in SQL syntax to explain your actions. Don't output anything else. If it is not possible to generate valid SQL, output an SQL comment saying so."#
    )
}

fn build_request_body(ai_prompt: String) -> impl serde::Serialize {
    json!({
        "model": "gpt-3.5-turbo",
        "messages": [{ "role": "user", "content": ai_prompt}],
        "temperature": 0.7,
    })
}

pub async fn make_llm_request(body: web::Json<AiPrompt>) -> Result<HttpResponse, LLMError> {
    let api_key = match &PARSEABLE.options.open_ai_key {
        Some(api_key) if api_key.len() > 3 => api_key,
        _ => return Err(LLMError::InvalidAPIKey),
    };

    let stream_name = &body.stream;
    let schema = PARSEABLE.get_stream(stream_name)?.get_schema();
    let filtered_schema = schema
        .flattened_fields()
        .into_iter()
        .map(Field::from)
        .collect_vec();

    let schema_json =
        serde_json::to_string(&filtered_schema).expect("always converted to valid json");

    let prompt = build_prompt(stream_name, &body.prompt, &schema_json);
    let body = build_request_body(prompt);

    let client = reqwest::Client::new();
    let response = client
        .post(OPEN_AI_URL)
        .header(header::CONTENT_TYPE, "application/json")
        .bearer_auth(api_key)
        .json(&body)
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
    #[error("{0}")]
    StreamDoesNotExist(#[from] StreamNotFound),
}

impl actix_web::ResponseError for LLMError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            Self::InvalidAPIKey => StatusCode::INTERNAL_SERVER_ERROR,
            Self::FailedRequest(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::APIError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::StreamDoesNotExist(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }
}
