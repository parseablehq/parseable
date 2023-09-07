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

use actix_web::{web, HttpResponse, Result};
use reqwest;
use serde::{Deserialize, Serialize};
use serde_json::json;

const OPEN_AI_URL: &str = "https://api.openai.com/v1/chat/completions";

#[derive(Serialize)]
struct RequestData {
    model: String,
    messages: Vec<Message>,
    temperature: f32,
}

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

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AiPrompt {
    prompt: String,
}

pub async fn make_llm_request(data: web::Json<AiPrompt>) -> Result<HttpResponse> {
    // let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY not set");
    let req = data.into_inner();
    let prompt = req.prompt;
    
    let api_key = "...";
  

    let json_data = json!({
        "model": "gpt-3.5-turbo",
        "messages": [{ "role": "user", "content": prompt}],
        "temperature": 0.5,
    });

    let client = reqwest::Client::new();
    let response = client
        .post(OPEN_AI_URL)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", api_key))
        .json(&json_data)
        .send()
        .await;

    match response {
        Ok(res) => {
            if res.status().is_success() {
                let body = res.json::<ResponseData>().await.unwrap();
                Ok(HttpResponse::Ok().json(&body.choices[0].message.content))
            } else {
                Ok(HttpResponse::InternalServerError().json(ErrorResponse {
                    error: "Failed to make the API request".to_string(),
                }))
            }
        }
        Err(err) => {
            eprintln!("Error: {:?}", err);
            Ok(HttpResponse::InternalServerError().json(ErrorResponse {
                error: "Internal server error".to_string(),
            }))
        }
    }
}
