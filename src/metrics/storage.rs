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

use crate::metrics::METRICS_NAMESPACE;
use actix_web_prometheus::PrometheusMetrics;
use once_cell::sync::Lazy;
use prometheus::{CounterVec, HistogramOpts, HistogramVec, Opts};

// Global storage metric used by all storage providers
pub static STORAGE_REQUEST_RESPONSE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new("storage_request_response_time", "Storage Request Latency")
            .namespace(METRICS_NAMESPACE),
        &["provider", "method", "status"],
    )
    .expect("metric can be created")
});

// Global storage metric for tracking number of files scanned
pub static STORAGE_FILES_SCANNED: Lazy<CounterVec> = Lazy::new(|| {
    CounterVec::new(
        Opts::new(
            "storage_files_scanned_total",
            "Total number of files scanned in storage operations",
        )
        .namespace(METRICS_NAMESPACE),
        &["provider", "operation"],
    )
    .expect("metric can be created")
});

pub static STORAGE_FILES_SCANNED_DATE: Lazy<CounterVec> = Lazy::new(|| {
    CounterVec::new(
        Opts::new(
            "storage_files_scanned_date_total",
            "Total number of files scanned in storage operations by date",
        )
        .namespace(METRICS_NAMESPACE),
        &["provider", "operation", "date"],
    )
    .expect("metric can be created")
});

pub trait StorageMetrics {
    fn register_metrics(&self, handler: &PrometheusMetrics);
}

pub mod localfs {
    use crate::{metrics::storage::STORAGE_FILES_SCANNED_DATE, storage::FSConfig};

    use super::{STORAGE_FILES_SCANNED, STORAGE_REQUEST_RESPONSE_TIME, StorageMetrics};

    impl StorageMetrics for FSConfig {
        fn register_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
            handler
                .registry
                .register(Box::new(STORAGE_REQUEST_RESPONSE_TIME.clone()))
                .expect("metric can be registered");
            handler
                .registry
                .register(Box::new(STORAGE_FILES_SCANNED.clone()))
                .expect("metric can be registered");
            handler
                .registry
                .register(Box::new(STORAGE_FILES_SCANNED_DATE.clone()))
                .expect("metric can be registered");
        }
    }
}

pub mod s3 {
    use crate::{metrics::storage::STORAGE_FILES_SCANNED_DATE, storage::S3Config};

    use super::{STORAGE_FILES_SCANNED, STORAGE_REQUEST_RESPONSE_TIME, StorageMetrics};

    impl StorageMetrics for S3Config {
        fn register_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
            handler
                .registry
                .register(Box::new(STORAGE_REQUEST_RESPONSE_TIME.clone()))
                .expect("metric can be registered");
            handler
                .registry
                .register(Box::new(STORAGE_FILES_SCANNED.clone()))
                .expect("metric can be registered");
            handler
                .registry
                .register(Box::new(STORAGE_FILES_SCANNED_DATE.clone()))
                .expect("metric can be registered");
        }
    }
}

pub mod azureblob {
    use crate::{metrics::storage::STORAGE_FILES_SCANNED_DATE, storage::AzureBlobConfig};

    use super::{STORAGE_FILES_SCANNED, STORAGE_REQUEST_RESPONSE_TIME, StorageMetrics};

    impl StorageMetrics for AzureBlobConfig {
        fn register_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
            handler
                .registry
                .register(Box::new(STORAGE_REQUEST_RESPONSE_TIME.clone()))
                .expect("metric can be registered");
            handler
                .registry
                .register(Box::new(STORAGE_FILES_SCANNED.clone()))
                .expect("metric can be registered");
            handler
                .registry
                .register(Box::new(STORAGE_FILES_SCANNED_DATE.clone()))
                .expect("metric can be registered");
        }
    }
}

pub mod gcs {
    use crate::{metrics::storage::STORAGE_FILES_SCANNED_DATE, storage::GcsConfig};

    use super::{STORAGE_FILES_SCANNED, STORAGE_REQUEST_RESPONSE_TIME, StorageMetrics};

    impl StorageMetrics for GcsConfig {
        fn register_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
            handler
                .registry
                .register(Box::new(STORAGE_REQUEST_RESPONSE_TIME.clone()))
                .expect("metric can be registered");
            handler
                .registry
                .register(Box::new(STORAGE_FILES_SCANNED.clone()))
                .expect("metric can be registered");
            handler
                .registry
                .register(Box::new(STORAGE_FILES_SCANNED_DATE.clone()))
                .expect("metric can be registered");
        }
    }
}
