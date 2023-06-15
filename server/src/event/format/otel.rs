mod common;
mod log;
mod resource;
mod trace;

pub use log::LogsData;
pub use trace::TracesData;

impl TryFrom<&TracesData> for serde_json::Value {
    type Error = serde_json::Error;

    fn try_from(value: &TracesData) -> Result<Self, Self::Error> {
        serde_json::to_value(value)
    }
}
