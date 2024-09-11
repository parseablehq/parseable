use std::{collections::HashMap, future::Future, pin::Pin};

use actix_web::{
    web::{self, Json},
    FromRequest, HttpRequest, Responder,
};
use http::HeaderMap;
use serde_json::Value;
use trino_response::QueryResponse;

use crate::option::CONFIG;

use super::query::QueryError;

#[derive(Debug, serde::Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryResultsTrino {
    pub id: String,
    pub next_uri: Option<String>,
    pub stats: Value,
    pub error: Option<Value>,
    pub warnings: Option<Value>,
    pub columns: Option<Value>,
    pub data: Option<Value>,
}

/// Query Request through http endpoint.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoQuery {
    pub query: String,
    #[serde(skip)]
    pub fields: bool,
}

impl FromRequest for TrinoQuery {
    type Error = actix_web::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let query = Json::<TrinoQuery>::from_request(req, payload);
        let params = web::Query::<HashMap<String, bool>>::from_request(req, payload)
            .into_inner()
            .map(|x| x.0)
            .unwrap_or_default();

        let fut = async move {
            let mut query = query.await?.into_inner();
            // format output json to include field names
            query.fields = params.get("fields").cloned().unwrap_or(false);

            Ok(query)
        };

        Box::pin(fut)
    }
}

pub async fn trino_query(
    _req: HttpRequest,
    query_request: TrinoQuery,
) -> Result<impl Responder, QueryError> {
    let sql = query_request.query;

    let catalog = if let Some(catalog) = &CONFIG.parseable.trino_catalog.as_ref() {
        catalog.to_owned()
    } else {
        return Err(QueryError::Anyhow(anyhow::Error::msg(
            "Value for P_TRINO_CATALOG_NAME required",
        )));
    };
    let schema = if let Some(schema) = &CONFIG.parseable.trino_schema.as_ref() {
        schema.to_owned()
    } else {
        return Err(QueryError::Anyhow(anyhow::Error::msg(
            "Value for P_TRINO_SCHEMA required",
        )));
    };
    let username = if let Some(username) = &CONFIG.parseable.trino_username.as_ref() {
        username.to_owned()
    } else {
        return Err(QueryError::Anyhow(anyhow::Error::msg(
            "Value for P_TRINO_USER_NAME required",
        )));
    };
    let auth = &CONFIG.parseable.trino_auth;
    let endpoint = if let Some(endpoint) = &CONFIG.parseable.trino_endpoint.as_ref() {
        if endpoint.ends_with("/") {
            &endpoint[0..endpoint.len() - 1].to_owned()
        } else {
            endpoint.to_owned()
        }
    } else {
        return Err(QueryError::Anyhow(anyhow::Error::msg(
            "Value for P_TRINO_ENDPOINT required",
        )));
    };

    trino_init(
        &sql,
        query_request.fields,
        endpoint,
        catalog,
        schema,
        username,
        auth,
    )
    .await?
    .to_http()
}

pub async fn trino_get(
    with_fields: bool,
    query_results: QueryResultsTrino,
) -> Result<QueryResponse, QueryError> {
    // initial check for nextUri
    if let Some(mut next_uri) = query_results.next_uri {
        let mut records: Vec<Value> = Vec::new();
        let mut fields: Vec<String> = Vec::new();

        let client = reqwest::Client::new();

        // loop will handle batches being sent by server
        loop {
            let res: QueryResultsTrino = client.get(next_uri.clone()).send().await?.json().await?;

            // check if columns and data present, collate
            // if len of fields is not 0, then don't overwrite
            if fields.is_empty() {
                if let Some(columns) = res.columns {
                    columns.as_array().unwrap().iter().for_each(|row| {
                        let name = row
                            .as_object()
                            .unwrap()
                            .get("name")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .to_string();
                        fields.push(name);
                    });
                }
            }

            if let Some(data) = res.data {
                if let Some(data) = data.as_array() {
                    data.iter().for_each(|d| records.push(d.to_owned()));
                }
            }

            // check if more data present
            if res.next_uri.is_some() {
                // more data to process
                next_uri = res.next_uri.unwrap().to_string();
            } else {
                // check if state is FINISHED or FAILED, then return
                let state = res
                    .stats
                    .as_object()
                    .unwrap()
                    .get("state")
                    .unwrap()
                    .as_str()
                    .unwrap();

                match state {
                    "FAILED" => {
                        // extract error
                        if res.error.is_some() {
                            let message = res
                                .error
                                .unwrap()
                                .as_object()
                                .unwrap()
                                .get("message")
                                .unwrap()
                                .to_string();
                            return Err(QueryError::Anyhow(anyhow::Error::msg(message)));
                        } else {
                            return Err(QueryError::Anyhow(anyhow::Error::msg("FAILED")));
                        }
                    }
                    "FINISHED" => {
                        // break
                        break;
                    }
                    _ => {
                        log::warn!("state '{state}' not covered");
                        break;
                    }
                }
            }
        }

        Ok(QueryResponse {
            trino_records: Some(records),
            fields,
            with_fields,
        })
    } else {
        // initial check for nex_uri retuned None
        // check for error messages
        Err(QueryError::Anyhow(anyhow::Error::msg(
            "Did not receive nexUri for initial QueryResults",
        )))
    }
}

#[allow(clippy::too_many_arguments)]
/// This is the entry point for a trino bound request
/// The first POST request will happen here and the subsequent GET requests will happen in `trino_get()`
pub async fn trino_init(
    query: &str,
    fields: bool,
    endpoint: &str,
    catalog: &str,
    schema: &str,
    user: &str,
    auth: &Option<String>,
) -> Result<QueryResponse, QueryError> {
    let mut headers = HeaderMap::new();
    headers.insert("x-trino-schema", schema.parse().unwrap());
    headers.insert("x-trino-catalog", catalog.parse().unwrap());
    headers.insert("x-trino-user", user.parse().unwrap());

    // add password if present
    if let Some(auth) = auth {
        headers.insert("Authorization", format!("Basic {auth}").parse().unwrap());
    }

    let response: QueryResultsTrino = match reqwest::Client::new()
        .post(format!("{endpoint}/v1/statement"))
        .body(query.to_owned())
        .headers(headers)
        .send()
        .await
    {
        Ok(r) => r.json().await?,
        Err(e) => return Err(QueryError::Anyhow(anyhow::Error::msg(e.to_string()))),
    };

    trino_get(fields, response).await
}

mod trino_response {
    use actix_web::{web, Responder};
    use itertools::Itertools;
    use serde_json::{json, Map, Value};

    use crate::handlers::http::query::QueryError;

    pub struct QueryResponse {
        pub trino_records: Option<Vec<Value>>,
        pub fields: Vec<String>,
        pub with_fields: bool,
    }

    impl QueryResponse {
        pub fn to_http(&self) -> Result<impl Responder, QueryError> {
            log::info!("{}", "Returning query results");
            let values = if let Some(trino_records) = self.trino_records.clone() {
                // trino_records = Vec<Array[Value]>
                let mut json_records: Vec<Map<String, Value>> = Vec::new();
                for array in trino_records.into_iter() {
                    let mut m: Map<String, Value> = Map::new();
                    for (key, val) in self
                        .fields
                        .clone()
                        .into_iter()
                        .zip(array.as_array().unwrap())
                    {
                        m.insert(key, val.clone());
                    }
                    json_records.push(m);
                }

                json_records.into_iter().map(Value::Object).collect_vec()
            } else {
                return Err(QueryError::Anyhow(anyhow::Error::msg(
                    "QueryResponse made improperly",
                )));
            };

            let response = if self.with_fields {
                json!({
                    "fields": self.fields,
                    "records": values
                })
            } else {
                Value::Array(values)
            };

            Ok(web::Json(response))
        }
    }
}
