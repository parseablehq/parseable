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

use std::{
    ops::Range,
    task::{Context, Poll},
    time,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream::BoxStream};
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as ObjectStoreResult, path::Path,
};

/* NOTE: Keeping these imports as they would make migration to object_store 0.10.0 easier
use object_store::{MultipartUpload, PutMultipartOpts, PutPayload}
*/

use crate::metrics::storage::STORAGE_REQUEST_RESPONSE_TIME;

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
}

impl<T: ObjectStore> MetricLayer<T> {
    pub fn new(inner: T, provider: &str) -> Self {
        Self {
            inner,
            provider: provider.to_string(),
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
    /// PutPayload.from_bytes(bytes)
    async fn put(
        &self,
        location: &Path,
        bytes: PutPayload, /* PutPayload */
    ) -> ObjectStoreResult<PutResult> {
        let time = time::Instant::now();
        let put_result = self.inner.put(location, bytes).await;
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

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload, /* PutPayload */
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let time = time::Instant::now();
        let put_result = self.inner.put_opts(location, payload, opts).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &put_result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "PUT_OPTS", status])
            .observe(elapsed);
        put_result
    }

    // // ! removed in object_store 0.10.0
    // async fn abort_multipart(
    //     &self,
    //     location: &Path,
    //     multipart_id: &MultipartId,
    // ) -> object_store::Result<()> {
    //     let time = time::Instant::now();
    //     let elapsed = time.elapsed().as_secs_f64();
    //     self.inner.abort_multipart(location, multipart_id).await?;
    //     STORAGE_REQUEST_RESPONSE_TIME
    //         .with_label_values(&["PUT_MULTIPART_ABORT", "200"])
    //         .observe(elapsed);
    //     Ok(())
    // }

    /* Keep for easier migration to object_store 0.10.0 */
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let time = time::Instant::now();
        let result = self.inner.put_multipart_opts(location, opts).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "PUT_MULTIPART_OPTS", status])
            .observe(elapsed);
        result
    }

    // todo completly tracking multipart upload
    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> /* ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> */
    {
        let time = time::Instant::now();
        let result = self.inner.put_multipart(location).await;
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

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let time = time::Instant::now();
        let get_result = self.inner.get(location).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &get_result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "GET", status])
            .observe(elapsed);
        get_result
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let time = time::Instant::now();
        let result = self.inner.get_opts(location, options).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "GET_OPTS", status])
            .observe(elapsed);
        result
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        let time = time::Instant::now();
        let result = self.inner.get_range(location, range).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "GET_RANGE", status])
            .observe(elapsed);
        result
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
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

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let time = time::Instant::now();
        let result = self.inner.head(location).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "HEAD", status])
            .observe(elapsed);
        result
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let time = time::Instant::now();
        let result = self.inner.delete(location).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "DELETE", status])
            .observe(elapsed);
        result
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, ObjectStoreResult<Path>>,
    ) -> BoxStream<'a, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        let time = time::Instant::now();
        let inner = self.inner.list(prefix);
        let res = StreamMetricWrapper {
            time,
            labels: ["LIST", "200"],
            inner,
        };
        Box::pin(res)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        let time = time::Instant::now();
        let inner = self.inner.list_with_offset(prefix, offset);
        let res = StreamMetricWrapper {
            time,
            labels: ["LIST_OFFSET", "200"],
            inner,
        };

        Box::pin(res)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
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

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let time = time::Instant::now();
        let result = self.inner.copy(from, to).await;
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

    async fn rename(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let time = time::Instant::now();
        let result = self.inner.rename(from, to).await;
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

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let time = time::Instant::now();
        let result = self.inner.copy_if_not_exists(from, to).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "COPY_IF", status])
            .observe(elapsed);
        result
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let time = time::Instant::now();
        let result = self.inner.rename_if_not_exists(from, to).await;
        let elapsed = time.elapsed().as_secs_f64();

        let status = match &result {
            Ok(_) => "200",
            Err(err) => error_to_status_code(err),
        };

        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&[&self.provider, "RENAME_IF", status])
            .observe(elapsed);
        result
    }
}

struct StreamMetricWrapper<'a, const N: usize, T> {
    time: time::Instant,
    labels: [&'static str; N],
    inner: BoxStream<'a, T>,
}

impl<T, const N: usize> Stream for StreamMetricWrapper<'_, N, T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            t @ Poll::Ready(None) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&self.labels)
                    .observe(self.time.elapsed().as_secs_f64());
                t
            }
            t => t,
        }
    }
}
