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

use crate::event::Event;
use crate::handlers::http::ingest::push_logs_unchecked;
use crate::handlers::http::query::Query as QueryJson;
use crate::parseable::PARSEABLE;
use crate::query::stream_schema_provider::{extract_primary_filter, is_within_staging_window};
use crate::{handlers::http::modal::IngestorMetadata, option::Mode};

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{FlightData, Ticket};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_select::concat::concat_batches;
use datafusion::logical_expr::BinaryExpr;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use futures::{TryStreamExt, stream};

use tonic::{Request, Response, Status};

use arrow_flight::FlightClient;
// use http::Uri;
use tonic::transport::{Channel, Uri};

pub type DoGetStream = stream::BoxStream<'static, Result<FlightData, Status>>;

pub fn get_query_from_ticket(req: &Request<Ticket>) -> Result<QueryJson, Box<Status>> {
    serde_json::from_slice::<QueryJson>(&req.get_ref().ticket)
        .map_err(|err| Box::new(Status::internal(err.to_string())))
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
    let schema = PARSEABLE
        .get_stream(stream_name)
        .map_err(|err| Status::failed_precondition(format!("Metadata Error: {err}")))?
        .get_schema();
    let rb = concat_batches(&schema, minute_result)
        .map_err(|err| Status::failed_precondition(format!("ArrowError: {err}")))?;

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
    let time_filters =
        extract_primary_filter(&[Expr::BinaryExpr(ex1), Expr::BinaryExpr(ex2)], &None);
    (PARSEABLE.options.mode == Mode::Query || PARSEABLE.options.mode == Mode::Prism)
        && is_within_staging_window(&time_filters)
}

fn lit_timestamp_milli(time: i64) -> Expr {
    Expr::Literal(ScalarValue::TimestampMillisecond(Some(time), None))
}

pub fn into_flight_data(records: Vec<RecordBatch>) -> Result<Response<DoGetStream>, Box<Status>> {
    let input_stream = futures::stream::iter(records.into_iter().map(Ok));
    let write_options = IpcWriteOptions::default()
        .try_with_compression(Some(arrow_ipc::CompressionType(1)))
        .map_err(|err| Status::failed_precondition(err.to_string()))?;

    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_max_flight_data_size(usize::MAX)
        .with_options(write_options)
        // .with_schema(schema.into())
        .build(input_stream);

    let flight_data_stream = flight_data_stream.map_err(|err| Status::unknown(err.to_string()));

    Ok(Response::new(Box::pin(flight_data_stream) as DoGetStream))
}
