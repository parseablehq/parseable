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

use crate::metrics::{STORAGE_REQUEST_RESPONSE_TIME, STORAGE_REQUESTS_INFLIGHT};

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
        result
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
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
    use object_store::{Attribute, Attributes, memory::InMemory, path::Path};

    use super::MetricLayer;

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
