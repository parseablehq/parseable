use itertools::Itertools;

use super::common::{attributes_to_string, id_to_string, KeyValue, Resource, Scope};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LogData {
    resource_attributes: String,
    scope_name: String,
    scope_version: String,
    scope_attributes: String,
    time_unix_nano: u64,
    observed_time_unix_nano: u64,
    severity_number: i32,
    severity_text: String,
    body: String,
    attributes: String,
    flags: u32,
    trace_id: Option<String>,
    span_id: Option<String>,
}

impl From<super::proto::log::LogsData> for Vec<LogData> {
    fn from(value: super::proto::log::LogsData) -> Self {
        let mut res = Vec::new();
        for resource_logs in value.resource_logs {
            let resource = resource_logs.resource.map(Resource::from);
            for scope_logs in resource_logs.scope_logs {
                let scope = scope_logs.scope.map(Scope::from);
                for log in scope_logs.log_records {
                    let log = LogData {
                        resource_attributes: resource
                            .as_ref()
                            .map(|x| attributes_to_string(&x.attributes))
                            .unwrap_or_default(),
                        scope_name: scope
                            .as_ref()
                            .map(|ref x| x.name.clone())
                            .unwrap_or_default(),
                        scope_version: scope
                            .as_ref()
                            .map(|ref x| x.version.clone())
                            .unwrap_or_default(),
                        scope_attributes: scope
                            .as_ref()
                            .map(|x| attributes_to_string(&x.attributes))
                            .unwrap_or_default(),
                        trace_id: log.trace_id.get(0..16).map(id_to_string),
                        span_id: log.span_id.get(0..8).map(id_to_string),
                        attributes: attributes_to_string(
                            &log.attributes
                                .into_iter()
                                .map(|x| Into::<KeyValue>::into(x))
                                .collect_vec(),
                        ),
                        time_unix_nano: log.time_unix_nano,
                        observed_time_unix_nano: log.observed_time_unix_nano,
                        severity_number: log.severity_number,
                        severity_text: log.severity_text,
                        body: log.body.map(|x| x.to_string()).unwrap_or_default(),
                        flags: log.flags,
                    };

                    res.push(log)
                }
            }
        }
        res
    }
}
