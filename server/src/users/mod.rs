pub mod dashboards;
pub mod filters;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct TimeFilter {
    to: String,
    from: String,
}
