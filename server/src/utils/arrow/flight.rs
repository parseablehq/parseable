use crate::handlers::http::query::Query as QueryJson;
use crate::{
    handlers::http::modal::IngestorMetadata,
    option::{Mode, CONFIG},
};
use arrow_array::RecordBatch;
use arrow_flight::Ticket;
use futures::TryStreamExt;
use serde_json::Value as JsonValue;
use tonic::{Request, Status};

use arrow_flight::FlightClient;
use http::Uri;
use tonic::transport::Channel;

pub fn get_query_from_ticket(req: Request<Ticket>) -> Result<QueryJson, Status> {
    if CONFIG.parseable.mode == Mode::Ingest {
        let query = serde_json::from_slice::<JsonValue>(&req.into_inner().ticket)
            .map_err(|_| Status::failed_precondition("Ticket is not valid json"))?["query"]
            .as_str()
            .ok_or_else(|| Status::failed_precondition("query is not valid string"))?
            .to_owned();
        Ok(QueryJson {
            query,
            send_null: false,
            fields: false,
            filter_tags: None,
            // we can use humantime because into_query handle parseing
            end_time: String::from("now"),
            start_time: String::from("1min"),
        })
    } else {
        Ok(
            serde_json::from_slice::<QueryJson>(&req.into_inner().ticket)
                .map_err(|err| Status::internal(err.to_string()))?,
        )
    }
}

pub async fn run_do_get_rpc(im: IngestorMetadata, sql: String) -> Result<Vec<RecordBatch>, Status> {
    let url = im
        .domain_name
        .rsplit_once(':')
        .ok_or(Status::failed_precondition(
            "Ingester metadata is courupted",
        ))?
        .0;
    let url = format!("{}:{}", url, im.flight_port);
    let url = url
        .parse::<Uri>()
        .map_err(|_| Status::failed_precondition("Ingester metadata is courupted"))?;
    let channel = Channel::builder(url)
        .connect()
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))?;

    let mut client = FlightClient::new(channel);
    client.add_header("authorization", &im.token)?;

    let response = client
        .do_get(Ticket {
            ticket: sql.clone().into(),
        })
        .await?;

    Ok(response.try_collect().await?)
}
