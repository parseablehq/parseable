use crate::event::format::ArrowSchema;
use arrow_schema::{DataType, Field};

use super::proto::{
    common::{InstrumentationScope, KeyValue},
    resource::Resource,
    trace::{
        span::{Event, Link},
        Status,
    },
};

fn attribute_datatype() -> DataType {
    DataType::Map(
        Box::new(Field::new(
            "entries",
            DataType::Struct(KeyValue::arrow_schema()),
            false,
        )),
        false,
    )
}

impl ArrowSchema for InstrumentationScope {
    fn arrow_schema() -> Vec<Field> {
        vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
            Field::new("attribute", DataType::Utf8, true),
            Field::new("dropped_attributes_count", DataType::UInt32, true),
        ]
    }
}

impl ArrowSchema for Resource {
    fn arrow_schema() -> Vec<Field> {
        vec![
            Field::new("attribute", DataType::Utf8, true),
            Field::new("dropped_attributes_count", DataType::UInt32, true),
        ]
    }
}

impl ArrowSchema for Event {
    fn arrow_schema() -> Vec<Field> {
        vec![
            Field::new("time_unix_nano", DataType::UInt64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("attribute", DataType::Utf8, true),
            Field::new("dropped_attributes_count", DataType::UInt32, true),
        ]
    }
}

impl ArrowSchema for Status {
    fn arrow_schema() -> Vec<Field> {
        vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("code", DataType::Int32, true),
        ]
    }
}

impl ArrowSchema for KeyValue {
    fn arrow_schema() -> Vec<Field> {
        vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::UInt32, false),
        ]
    }
}

impl ArrowSchema for Link {
    fn arrow_schema() -> Vec<Field> {
        vec![
            Field::new("trace_id", DataType::FixedSizeBinary(16), true),
            Field::new("span_id", DataType::FixedSizeBinary(8), true),
            Field::new("trace_state", DataType::Utf8, true),
            Field::new("attributes", attribute_datatype(), true),
            Field::new("dropped_attributes_count", DataType::UInt32, true),
        ]
    }
}

impl ArrowSchema for super::proto::trace::TracesData {
    fn arrow_schema() -> Vec<Field> {
        vec![
            Field::new("resource", DataType::Struct(Resource::arrow_schema()), true),
            Field::new(
                "scope",
                DataType::Struct(InstrumentationScope::arrow_schema()),
                true,
            ),
            Field::new("trace_id", DataType::FixedSizeBinary(16), true),
            Field::new("span_id", DataType::FixedSizeBinary(8), true),
            Field::new("trace_state", DataType::Utf8, true),
            Field::new("parent_span_id", DataType::FixedSizeBinary(8), true),
            Field::new("name", DataType::Utf8, true),
            Field::new("kind", DataType::Int32, true),
            Field::new("start_time_unix_nano", DataType::UInt64, true),
            Field::new("end_time_unix_nano", DataType::UInt64, true),
            Field::new("attributes", attribute_datatype(), true),
            Field::new("dropped_attributes_count", DataType::UInt32, true),
            Field::new(
                "events",
                DataType::List(Box::new(Field::new(
                    "items",
                    DataType::Struct(Event::arrow_schema()),
                    true,
                ))),
                true,
            ),
            Field::new("dropped_events_count", DataType::UInt32, true),
            Field::new(
                "links",
                DataType::List(Box::new(Field::new(
                    "items",
                    DataType::Struct(Link::arrow_schema()),
                    true,
                ))),
                true,
            ),
            Field::new("dropped_links_count", DataType::UInt32, true),
            Field::new("status", DataType::Struct(Status::arrow_schema()), true),
        ]
    }
}
