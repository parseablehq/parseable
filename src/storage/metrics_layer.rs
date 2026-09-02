/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::{
    ops::Range,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
    time,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream::BoxStream};
use object_store::{
    Attribute, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult, path::Path,
};

use crate::metrics::{
    STORAGE_READ_BYTES_TOTAL, STORAGE_READ_RANGES_TOTAL, STORAGE_REQUEST_RESPONSE_TIME,
    STORAGE_REQUESTS_INFLIGHT,
};

const READ_METHODS: [&str; 2] = ["GET", "GET_RANGES"];
const STORAGE_STATUSES: [&str; 9] = [
    "200", "304", "400", "401", "404", "409", "412", "500", "501",
];

/// Map the configured storage provider name to the label used by MetricLayer.
pub fn storage_metrics_provider_label(provider_name: &str) -> Option<&'static str> {
    match provider_name {
        "gcs" => Some("gcs"),
        "s3" => Some("s3"),
        "blob-store" | "azure_blob" => Some("azure_blob"),
        _ => None,
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
struct StorageMetricSnapshot {
    read_bytes: u64,
    read_ranges: u64,
    get_requests: u64,
    get_ranges_requests: u64,
    response_time_sum_seconds: f64,
    response_time_count: u64,
    non_2xx_requests: u64,
    inflight: u64,
}

fn read_inflight(provider: &str) -> u64 {
    READ_METHODS
        .iter()
        .map(|method| {
            STORAGE_REQUESTS_INFLIGHT
                .with_label_values(&[provider, method])
                .get()
                .max(0) as u64
        })
        .sum()
}

impl StorageMetricSnapshot {
    fn capture(provider: &str) -> Self {
        let mut snapshot = Self {
            read_bytes: STORAGE_READ_BYTES_TOTAL
                .with_label_values(&[provider])
                .get(),
            read_ranges: STORAGE_READ_RANGES_TOTAL
                .with_label_values(&[provider])
                .get(),
            inflight: read_inflight(provider),
            ..Self::default()
        };
        for method in READ_METHODS {
            for status in STORAGE_STATUSES {
                let histogram =
                    STORAGE_REQUEST_RESPONSE_TIME.with_label_values(&[provider, method, status]);
                let count = histogram.get_sample_count();
                match method {
                    "GET" => snapshot.get_requests += count,
                    "GET_RANGES" => snapshot.get_ranges_requests += count,
                    _ => unreachable!(),
                }
                snapshot.response_time_count += count;
                snapshot.response_time_sum_seconds += histogram.get_sample_sum();
                if !status.starts_with('2') {
                    snapshot.non_2xx_requests += count;
                }
            }
        }
        snapshot
    }

    fn delta_to(&self, end: &Self, peak_inflight_sampled: u64) -> StorageMetricDelta {
        StorageMetricDelta {
            read_bytes: end.read_bytes.saturating_sub(self.read_bytes),
            read_ranges: end.read_ranges.saturating_sub(self.read_ranges),
            get_requests: end.get_requests.saturating_sub(self.get_requests),
            get_ranges_requests: end
                .get_ranges_requests
                .saturating_sub(self.get_ranges_requests),
            response_time_sum_seconds: (end.response_time_sum_seconds
                - self.response_time_sum_seconds)
                .max(0.0),
            response_time_count: end
                .response_time_count
                .saturating_sub(self.response_time_count),
            non_2xx_requests: end.non_2xx_requests.saturating_sub(self.non_2xx_requests),
            peak_inflight_sampled,
        }
    }
}

/// Process-global storage metric deltas observed during a branch wall-clock
/// window. Concurrent process activity is intentionally included.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StorageMetricDelta {
    pub read_bytes: u64,
    pub read_ranges: u64,
    pub get_requests: u64,
    pub get_ranges_requests: u64,
    pub response_time_sum_seconds: f64,
    pub response_time_count: u64,
    pub non_2xx_requests: u64,
    pub peak_inflight_sampled: u64,
}

impl StorageMetricDelta {
    pub fn response_time_average_ms(&self) -> f64 {
        if self.response_time_count == 0 {
            0.0
        } else {
            self.response_time_sum_seconds * 1000.0 / self.response_time_count as f64
        }
    }
}

/// Snapshot window over process-global storage metrics. `observe_peak` is
/// intended for a low-frequency sampler; it does not instrument requests.
#[derive(Debug)]
pub struct StorageMetricWindow {
    provider: String,
    start: StorageMetricSnapshot,
    peak_inflight_sampled: AtomicU64,
}

impl StorageMetricWindow {
    pub fn start(provider: &str) -> Self {
        let start = StorageMetricSnapshot::capture(provider);
        Self {
            provider: provider.to_string(),
            peak_inflight_sampled: AtomicU64::new(start.inflight),
            start,
        }
    }

    pub fn provider(&self) -> &str {
        &self.provider
    }

    pub fn observe_peak(&self) {
        self.record_inflight(read_inflight(&self.provider));
    }

    fn record_inflight(&self, inflight: u64) {
        self.peak_inflight_sampled
            .fetch_max(inflight, Ordering::Relaxed);
    }

    pub fn delta(&self) -> StorageMetricDelta {
        self.observe_peak();
        self.start.delta_to(
            &StorageMetricSnapshot::capture(&self.provider),
            self.peak_inflight_sampled.load(Ordering::Relaxed),
        )
    }
}

/// RAII guard that increments the in-flight gauge on construction and
/// decrements on drop. Handles early returns, panics, and dropped futures.
struct InflightGuard {
    provider: String,
    method: &'static str,
}

impl InflightGuard {
    fn new(provider: &str, method: &'static str) -> Self {
        STORAGE_REQUESTS_INFLIGHT
            .with_label_values(&[provider, method])
            .inc();
        Self {
            provider: provider.to_string(),
            method,
        }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        STORAGE_REQUESTS_INFLIGHT
            .with_label_values(&[&self.provider, self.method])
            .dec();
    }
}

// Public helper function to map object_store errors to HTTP status codes
pub fn error_to_status_code(err: &object_store::Error) -> &'static str {
    match err {
        // 400 Bad Request - Client errors
        object_store::Error::Generic { .. } => "400",

        // 401 Unauthorized - Authentication required
        object_store::Error::Unauthenticated { .. } => "401",

        // 404 Not Found - Resource doesn't exist
        object_store::Error::NotFound { .. } => "404",

        // 409 Conflict - Resource already exists
        object_store::Error::AlreadyExists { .. } => "409",

        // 412 Precondition Failed - If-Match, If-None-Match, etc. failed
        object_store::Error::Precondition { .. } => "412",

        // 304 Not Modified
        object_store::Error::NotModified { .. } => "304",

        // 501 Not Implemented - Feature not supported
        object_store::Error::NotSupported { .. } => "501",

        // 500 Internal Server Error - All other errors
        _ => "500",
    }
}

#[derive(Debug)]
pub struct MetricLayer<T: ObjectStore> {
    inner: T,
    provider: String,
    cache_control_no_store: bool,
}

impl<T: ObjectStore> MetricLayer<T> {
    pub fn new(inner: T, provider: &str) -> Self {
        Self {
            inner,
            provider: provider.to_string(),
            cache_control_no_store: false,
        }
    }

    pub fn with_cache_control_no_store(mut self, enabled: bool) -> Self {
        self.cache_control_no_store = enabled;
        self
    }

    fn set_cache_control(&self, location: &Path, attributes: &mut object_store::Attributes) {
        // Parquet data is immutable and may be cached, while mutable metadata
        // (manifests, schemas, stream metadata, etc.) must not be cached.
        if self.cache_control_no_store && !location.as_ref().ends_with(".parquet") {
            attributes.insert(Attribute::CacheControl, "no-store".into());
        }
    }
}

impl<T: ObjectStore> std::fmt::Display for MetricLayer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Metric({})", self.inner)
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for MetricLayer<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.set_cache_control(location, &mut opts.attributes);
        let _guard = InflightGuard::new(&self.provider, "PUT");
        let time = time::Instant::now();
        let put_result = self.inner.put_opts(location, payload, opts).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &put_result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "PUT", status])
            .observe(elapsed);
        put_result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        mut opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.set_cache_control(location, &mut opts.attributes);
        let _guard = InflightGuard::new(&self.provider, "PUT_MULTIPART");
        let time = time::Instant::now();
        let result = self.inner.put_multipart_opts(location, opts).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "PUT_MULTIPART", status])
            .observe(elapsed);
        result
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let requested_bytes = options.range.as_ref().and_then(|range| match range {
            object_store::GetRange::Bounded(range) => Some(range.end.saturating_sub(range.start)),
            object_store::GetRange::Suffix(bytes) => Some(*bytes),
            object_store::GetRange::Offset(_) => None,
        });
        STORAGE_READ_RANGES_TOTAL
            .with_label_values(&[&self.provider])
            .inc();
        let _guard = InflightGuard::new(&self.provider, "GET");
        let time = time::Instant::now();
        let result = self.inner.get_opts(location, options).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "GET", status])
            .observe(elapsed);
        let requested_bytes = requested_bytes.or_else(|| {
            result
                .as_ref()
                .ok()
                .map(|result| result.range.end.saturating_sub(result.range.start))
        });
        if let Some(bytes) = requested_bytes {
            STORAGE_READ_BYTES_TOTAL
                .with_label_values(&[&self.provider])
                .inc_by(bytes);
        }
        result
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        STORAGE_READ_BYTES_TOTAL
            .with_label_values(&[&self.provider])
            .inc_by(
                ranges
                    .iter()
                    .map(|range| range.end.saturating_sub(range.start))
                    .sum(),
            );
        STORAGE_READ_RANGES_TOTAL
            .with_label_values(&[&self.provider])
            .inc_by(ranges.len() as u64);
        let _guard = InflightGuard::new(&self.provider, "GET_RANGES");
        let time = time::Instant::now();
        let result = self.inner.get_ranges(location, ranges).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "GET_RANGES", status])
            .observe(elapsed);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let _guard = InflightGuard::new(&self.provider, "LIST");
        let time = time::Instant::now();
        let inner = self.inner.list(prefix);
        let res = StreamMetricWrapper {
            time,
            provider: self.provider.clone(),
            method: "LIST",
            status: "200",
            _guard,
            inner,
        };
        Box::pin(res)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let _guard = InflightGuard::new(&self.provider, "LIST_OFFSET");
        let time = time::Instant::now();
        let inner = self.inner.list_with_offset(prefix, offset);
        let res = StreamMetricWrapper {
            time,
            provider: self.provider.clone(),
            method: "LIST_OFFSET",
            status: "200",
            _guard,
            inner,
        };

        Box::pin(res)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let _guard = InflightGuard::new(&self.provider, "LIST_DELIM");
        let time = time::Instant::now();
        let result = self.inner.list_with_delimiter(prefix).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "LIST_DELIM", status])
            .observe(elapsed);
        result
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        let _guard = InflightGuard::new(&self.provider, "COPY");
        let time = time::Instant::now();
        let result = self.inner.copy_opts(from, to, options).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "COPY", status])
            .observe(elapsed);
        result
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        let _guard = InflightGuard::new(&self.provider, "RENAME");
        let time = time::Instant::now();
        let result = self.inner.rename_opts(from, to, options).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "RENAME", status])
            .observe(elapsed);
        result
    }
}

struct StreamMetricWrapper<'a, T> {
    time: time::Instant,
    provider: String,
    method: &'static str,
    status: &'static str,
    _guard: InflightGuard,
    inner: BoxStream<'a, T>,
}

impl<T> Stream for StreamMetricWrapper<'_, T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            t @ Poll::Ready(None) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&[&self.provider, self.method, self.status])
                    .observe(self.time.elapsed().as_secs_f64());
                t
            }
            t => t,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use object_store::{
        Attribute, Attributes, ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory,
        path::Path,
    };

    use super::{MetricLayer, StorageMetricSnapshot, StorageMetricWindow};
    use crate::metrics::{
        STORAGE_READ_BYTES_TOTAL, STORAGE_READ_RANGES_TOTAL, STORAGE_REQUEST_RESPONSE_TIME,
        STORAGE_REQUESTS_INFLIGHT,
    };

    // Process-global Prometheus vectors make snapshot-window tests inherently
    // process-global. Tests touching them must hold this lock even when unique
    // labels also isolate them from unrelated metric tests.
    static PROCESS_GLOBAL_METRIC_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn requested_read_bytes_and_ranges_are_counted_without_path_labels() {
        let provider = "hot-tier-metrics-test";
        let store = InMemory::new();
        store
            .put(&Path::from("file"), PutPayload::from_static(b"0123456789"))
            .await
            .unwrap();
        let layer = MetricLayer::new(store, provider);
        let bytes_before = STORAGE_READ_BYTES_TOTAL
            .with_label_values(&[provider])
            .get();
        let ranges_before = STORAGE_READ_RANGES_TOTAL
            .with_label_values(&[provider])
            .get();

        let result = layer
            .get_ranges(&Path::from("file"), &[0..2, 4..7])
            .await
            .unwrap();

        assert_eq!(result.concat(), b"01456");
        assert_eq!(
            STORAGE_READ_BYTES_TOTAL
                .with_label_values(&[provider])
                .get()
                - bytes_before,
            5
        );
        assert_eq!(
            STORAGE_READ_RANGES_TOTAL
                .with_label_values(&[provider])
                .get()
                - ranges_before,
            2
        );
    }

    #[test]
    fn process_window_metric_delta_arithmetic_is_saturating_and_derives_average() {
        let start = StorageMetricSnapshot {
            read_bytes: 100,
            read_ranges: 10,
            get_requests: 4,
            get_ranges_requests: 2,
            response_time_sum_seconds: 1.0,
            response_time_count: 6,
            non_2xx_requests: 1,
            inflight: 3,
        };
        let end = StorageMetricSnapshot {
            read_bytes: 350,
            read_ranges: 18,
            get_requests: 9,
            get_ranges_requests: 5,
            response_time_sum_seconds: 2.6,
            response_time_count: 14,
            non_2xx_requests: 3,
            inflight: 1,
        };
        let delta = start.delta_to(&end, 17);
        assert_eq!(delta.read_bytes, 250);
        assert_eq!(delta.read_ranges, 8);
        assert_eq!(delta.get_requests, 5);
        assert_eq!(delta.get_ranges_requests, 3);
        assert_eq!(delta.response_time_count, 8);
        assert!((delta.response_time_sum_seconds - 1.6).abs() < f64::EPSILON);
        assert!((delta.response_time_average_ms() - 200.0).abs() < f64::EPSILON);
        assert_eq!(delta.non_2xx_requests, 2);
        assert_eq!(delta.peak_inflight_sampled, 17);

        let reset = end.delta_to(&start, 1);
        assert_eq!(reset.read_bytes, 0);
        assert_eq!(reset.response_time_count, 0);
        assert_eq!(reset.response_time_sum_seconds, 0.0);
    }

    #[test]
    fn process_window_captures_metric_vectors_and_non_2xx() {
        let _serial = PROCESS_GLOBAL_METRIC_TEST_LOCK.lock().unwrap();
        // A unique provider label isolates this serialized process-global test
        // from production labels such as `gcs`.
        let provider = "process-window-metrics-test";
        let window = StorageMetricWindow::start(provider);
        STORAGE_READ_BYTES_TOTAL
            .with_label_values(&[provider])
            .inc_by(1_024);
        STORAGE_READ_RANGES_TOTAL
            .with_label_values(&[provider])
            .inc_by(5);
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[provider, "GET", "200"])
            .observe(0.1);
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[provider, "GET", "500"])
            .observe(0.3);
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[provider, "GET_RANGES", "200"])
            .observe(0.2);
        let inflight = STORAGE_REQUESTS_INFLIGHT.with_label_values(&[provider, "GET"]);
        inflight.add(7);
        window.observe_peak();
        inflight.sub(7);

        let delta = window.delta();
        assert_eq!(delta.read_bytes, 1_024);
        assert_eq!(delta.read_ranges, 5);
        assert_eq!(delta.get_requests, 2);
        assert_eq!(delta.get_ranges_requests, 1);
        assert_eq!(delta.response_time_count, 3);
        assert!((delta.response_time_sum_seconds - 0.6).abs() < 1e-12);
        assert!((delta.response_time_average_ms() - 200.0).abs() < 1e-9);
        assert_eq!(delta.non_2xx_requests, 1);
        assert_eq!(delta.peak_inflight_sampled, 7);
    }

    #[test]
    fn process_window_peak_tracking_is_monotonic() {
        let _serial = PROCESS_GLOBAL_METRIC_TEST_LOCK.lock().unwrap();
        // This unit test updates only the window-local peak atomic, but it uses
        // the same serialization rule as all StorageMetricWindow tests.
        let window = StorageMetricWindow {
            provider: "peak-unit-test".to_string(),
            start: StorageMetricSnapshot::default(),
            peak_inflight_sampled: 2.into(),
        };
        window.record_inflight(7);
        window.record_inflight(4);
        window.record_inflight(11);
        assert_eq!(
            window
                .peak_inflight_sampled
                .load(std::sync::atomic::Ordering::Relaxed),
            11
        );
    }

    #[test]
    fn storage_provider_names_map_to_metric_layer_labels() {
        assert_eq!(super::storage_metrics_provider_label("gcs"), Some("gcs"));
        assert_eq!(
            super::storage_metrics_provider_label("blob-store"),
            Some("azure_blob")
        );
        assert_eq!(super::storage_metrics_provider_label("drive"), None);
    }

    #[test]
    fn s3_metadata_uploads_disable_caching() {
        let layer = MetricLayer::new(InMemory::new(), "s3").with_cache_control_no_store(true);
        let mut attributes = Attributes::new();

        layer.set_cache_control(&Path::from("stream/stream.json"), &mut attributes);

        assert_eq!(
            attributes.get(&Attribute::CacheControl).map(AsRef::as_ref),
            Some("no-store")
        );
    }

    #[test]
    fn s3_parquet_uploads_remain_cacheable() {
        let layer = MetricLayer::new(InMemory::new(), "s3").with_cache_control_no_store(true);
        let mut attributes = Attributes::new();

        layer.set_cache_control(&Path::from("stream/events.parquet"), &mut attributes);

        assert_eq!(attributes.get(&Attribute::CacheControl), None);
    }

    #[test]
    fn cache_control_is_disabled_by_default() {
        let layer = MetricLayer::new(InMemory::new(), "s3");
        let mut attributes = Attributes::new();

        layer.set_cache_control(&Path::from("stream/stream.json"), &mut attributes);

        assert_eq!(attributes.get(&Attribute::CacheControl), None);
    }
}
