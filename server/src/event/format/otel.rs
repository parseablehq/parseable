mod proto;
mod schema;
mod trace;

use std::sync::Arc;

use arrow_schema::Schema;
use serde_json::Value;

pub use self::proto::log::LogsData;
pub use self::proto::trace::TracesData;
pub use self::trace::SpanData;

use super::{ArrowSchema, EventFormat};

pub struct TraceEvent {
    pub data: TracesData,
}

impl EventFormat for TraceEvent {
    fn decode(self) -> Result<super::RecordContext, anyhow::Error> {
        let body: Vec<SpanData> = self.data.into();
        let Value::Array(arr) = serde_json::to_value(&body)? else { unreachable!("serde serialized from vec of span data") };
        let rb = super::json::decode(arr, Arc::new(Schema::new(TracesData::arrow_schema())))?;
        Ok(super::RecordContext {
            is_first: false,
            rb,
        })
    }
}
