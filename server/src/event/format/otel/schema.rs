use crate::event::format::ArrowSchema;
use arrow_schema::{DataType, Field};

use super::{trace, TraceEvent};

fn attribute_datatype() -> DataType {
    DataType::Utf8
}

fn common_schema() -> Vec<Field> {
    vec![
        Field::new("resource_attributes", attribute_datatype(), true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_attributes", attribute_datatype(), true),
    ]
}

impl ArrowSchema for trace::Event {
    fn arrow_schema() -> Vec<Field> {
        vec![
            Field::new("time_unix_nano", DataType::UInt64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("attributes", attribute_datatype(), true),
        ]
    }
}

impl ArrowSchema for trace::Link {
    fn arrow_schema() -> Vec<Field> {
        vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("trace_state", DataType::Utf8, true),
            Field::new("attributes", attribute_datatype(), true),
        ]
    }
}

impl ArrowSchema for TraceEvent {
    fn arrow_schema() -> Vec<Field> {
        let mut schema = common_schema();
        schema.extend(vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("trace_state", DataType::Utf8, true),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("kind", DataType::Int32, true),
            Field::new("start_time_unix_nano", DataType::UInt64, true),
            Field::new("end_time_unix_nano", DataType::UInt64, true),
            Field::new("attributes", attribute_datatype(), true),
            Field::new(
                "events",
                DataType::List(Box::new(Field::new(
                    "items",
                    DataType::Struct(trace::Event::arrow_schema()),
                    true,
                ))),
                true,
            ),
            Field::new(
                "links",
                DataType::List(Box::new(Field::new(
                    "items",
                    DataType::Struct(trace::Link::arrow_schema()),
                    true,
                ))),
                true,
            ),
            Field::new("status_message", DataType::Utf8, true),
            Field::new("status_code", DataType::Int32, true),
        ]);

        schema
    }
}

impl ArrowSchema for super::LogEvent {
    fn arrow_schema() -> Vec<Field> {
        let mut schema = common_schema();
        schema.extend(vec![
            Field::new("time_unix_nano", DataType::UInt64, true),
            Field::new("observed_time_unix_nano", DataType::UInt64, true),
            Field::new("severity_number", DataType::Int32, true),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("attributes", attribute_datatype(), true),
            Field::new("flags", DataType::UInt32, true),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
        ]);
        schema
    }
}
