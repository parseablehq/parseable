use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct StatsCounter {
    ingestion_size: AtomicU64,
    storage_size: AtomicU64,
}

impl Default for StatsCounter {
    fn default() -> Self {
        Self {
            ingestion_size: AtomicU64::new(0),
            storage_size: AtomicU64::new(0),
        }
    }
}

impl PartialEq for StatsCounter {
    fn eq(&self, other: &Self) -> bool {
        self.ingestion_size() == other.ingestion_size()
            && self.storage_size() == other.storage_size()
    }
}

impl StatsCounter {
    pub fn new(ingestion_size: u64, storage_size: u64) -> Self {
        Self {
            ingestion_size: AtomicU64::new(ingestion_size),
            storage_size: AtomicU64::new(storage_size),
        }
    }

    pub fn ingestion_size(&self) -> u64 {
        self.ingestion_size.load(Ordering::Relaxed)
    }

    pub fn storage_size(&self) -> u64 {
        self.storage_size.load(Ordering::Relaxed)
    }

    pub fn add_ingestion_size(&self, size: u64) {
        self.ingestion_size.fetch_add(size, Ordering::AcqRel);
    }

    pub fn add_storage_size(&self, size: u64) {
        self.storage_size.fetch_add(size, Ordering::AcqRel);
    }
}

/// Helper struct type created by copying stats values from metadata
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub ingestion: u64,
    pub storage: u64,
}

impl From<&StatsCounter> for Stats {
    fn from(stats: &StatsCounter) -> Self {
        Self {
            ingestion: stats.ingestion_size(),
            storage: stats.storage_size(),
        }
    }
}

impl From<Stats> for StatsCounter {
    fn from(stats: Stats) -> Self {
        StatsCounter::new(stats.ingestion, stats.storage)
    }
}
