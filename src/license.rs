use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::storage::{IngestionQuotaType, QuotaPeriod};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LicenseState {
    Valid = 0,
    Invalid = 1,
    Expired = 2,
    IngestionQuotaExceeded = 3,
}

impl LicenseState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "Valid",
            Self::Invalid => "Invalid",
            Self::Expired => "Expired",
            Self::IngestionQuotaExceeded => "IngestionQuotaExceeded",
        }
    }
}

impl From<u8> for LicenseState {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Valid,
            2 => Self::Expired,
            3 => Self::IngestionQuotaExceeded,
            _ => Self::Invalid,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LicenseStatus {
    pub state: LicenseState,
    pub reason: String,
    pub checked_at: DateTime<Utc>,
    pub quota: Option<QuotaStatus>,
}

impl LicenseStatus {
    pub fn new(state: LicenseState, reason: impl Into<String>) -> Self {
        Self {
            state,
            reason: reason.into(),
            checked_at: Utc::now(),
            quota: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaStatus {
    pub quota_type: IngestionQuotaType,
    pub period: QuotaPeriod,
    pub used: u64,
    pub limit: u64,
    #[serde(default)]
    pub remaining: u64,
    pub exceeded: bool,
    pub period_start: Option<DateTime<Utc>>,
    pub period_end: Option<DateTime<Utc>>,
}
