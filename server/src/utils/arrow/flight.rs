use crate::event::Event;
use crate::handlers::http::ingest::push_logs_unchecked;
use crate::handlers::http::query::Query as QueryJson;
use crate::metadata::STREAM_INFO;
use crate::query::stream_schema_provider::include_now;
use crate::{
    handlers::http::modal::IngestorMetadata,
    option::{Mode, CONFIG},
};

use arrow_array::RecordBatch;
use arrow_flight::Ticket;
use arrow_select::concat::concat_batches;
use datafusion::logical_expr::BinaryExpr;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;

use tonic::{Request, Status};

use arrow_flight::FlightClient;
use http::Uri;
use tonic::transport::Channel;

pub fn get_query_from_ticket(req: Request<Ticket>) -> Result<QueryJson, Status> {
    serde_json::from_slice::<QueryJson>(&req.into_inner().ticket)
        .map_err(|err| Status::internal(err.to_string()))
}

pub async fn run_do_get_rpc(
    im: IngestorMetadata,
    ticket: String,
) -> Result<Vec<RecordBatch>, Status> {
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

    let client = FlightClient::new(channel);
    let inn = client
        .into_inner()
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);

    let mut client = FlightClient::new_from_inner(inn);

    client.add_header("authorization", &im.token)?;

    let response = client
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await?;

    Ok(response.try_collect().await?)
}

/// all the records from the ingesters are concatinated into one event and pushed to memory
pub async fn append_temporary_events(
    stream_name: &str,
    minute_result: Vec<&RecordBatch>,
) -> Result<
    //Vec<Event>
    Event,
    Status,
> {
    let schema = STREAM_INFO
        .schema(stream_name)
        .map_err(|err| Status::failed_precondition(format!("Metadata Error: {}", err)))?;
    let rb = concat_batches(&schema, minute_result)
        .map_err(|err| Status::failed_precondition(format!("ArrowError: {}", err)))?;

    let event = push_logs_unchecked(rb, stream_name)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    Ok(event)
}

pub fn send_to_ingester(start: i64, end: i64) -> bool {
    let filter_start = lit_timestamp_milli(
        start, //query.start.timestamp_millis()
    );
    let filter_end = lit_timestamp_milli(
        end, //query.end.timestamp_millis()
    );

    let expr_left = Expr::Column(datafusion::common::Column {
        relation: None,
        name: "p_timestamp".to_owned(),
    });

    let ex1 = BinaryExpr::new(
        Box::new(expr_left.clone()),
        datafusion::logical_expr::Operator::Gt,
        Box::new(filter_start),
    );
    let ex2 = BinaryExpr::new(
        Box::new(expr_left),
        datafusion::logical_expr::Operator::Lt,
        Box::new(filter_end),
    );
    let ex = [Expr::BinaryExpr(ex1), Expr::BinaryExpr(ex2)];

    CONFIG.parseable.mode == Mode::Query && include_now(&ex, None)
}

fn lit_timestamp_milli(time: i64) -> Expr {
    Expr::Literal(ScalarValue::TimestampMillisecond(Some(time), None))
}
