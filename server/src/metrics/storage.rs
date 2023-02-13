use actix_web_prometheus::PrometheusMetrics;

pub trait StorageMetrics {
    fn register_metrics(&self, handler: &PrometheusMetrics);
}

pub mod localfs {
    use crate::{metrics::METRICS_NAMESPACE, storage::FSConfig};
    use lazy_static::lazy_static;
    use prometheus::{HistogramOpts, HistogramVec};

    use super::StorageMetrics;

    lazy_static! {
        pub static ref REQUEST_RESPONSE_TIME: HistogramVec = HistogramVec::new(
            HistogramOpts::new("local_fs_response_time", "FileSystem Request Latency")
                .namespace(METRICS_NAMESPACE),
            &["method", "status"]
        )
        .expect("metric can be created");
    }

    impl StorageMetrics for FSConfig {
        fn register_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
            handler
                .registry
                .register(Box::new(REQUEST_RESPONSE_TIME.clone()))
                .expect("metric can be registered");
        }
    }
}

pub mod s3 {
    use crate::{metrics::METRICS_NAMESPACE, storage::S3Config};
    use lazy_static::lazy_static;
    use prometheus::{HistogramOpts, HistogramVec};

    use super::StorageMetrics;

    lazy_static! {
        pub static ref REQUEST_RESPONSE_TIME: HistogramVec = HistogramVec::new(
            HistogramOpts::new("s3_response_time", "S3 Request Latency")
                .namespace(METRICS_NAMESPACE),
            &["method", "status"]
        )
        .expect("metric can be created");
    }

    impl StorageMetrics for S3Config {
        fn register_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
            handler
                .registry
                .register(Box::new(REQUEST_RESPONSE_TIME.clone()))
                .expect("metric can be registered");
        }
    }
}
