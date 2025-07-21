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

use crate::metrics::storage::s3::QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME;

#[derive(Debug)]
pub struct MetricLayer<T: ObjectStore> {
    inner: T,
}

impl<T: ObjectStore> MetricLayer<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
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
        let put_result = self.inner.put(location, bytes).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT", "200"])
            .observe(elapsed);
        return Ok(put_result);
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload, /* PutPayload */
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let time = time::Instant::now();
        let put_result = self.inner.put_opts(location, payload, opts).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT_OPTS", "200"])
            .observe(elapsed);
        return Ok(put_result);
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
    //     QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
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
        let multipart_upload = self.inner.put_multipart_opts(location, opts).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT_MULTIPART_OPTS", "200"])
            .observe(elapsed);

        Ok(multipart_upload)
    }

    // todo completly tracking multipart upload
    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> /* ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> */
    {
        let time = time::Instant::now();
        let multipart_upload = self.inner.put_multipart(location).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT_MULTIPART", "200"])
            .observe(elapsed);

        Ok(multipart_upload)
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let time = time::Instant::now();
        let res = self.inner.get(location).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let time = time::Instant::now();
        let res = self.inner.get_opts(location, options).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["GET_OPTS", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        let time = time::Instant::now();
        let res = self.inner.get_range(location, range).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["GET_RANGE", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        let time = time::Instant::now();
        let res = self.inner.get_ranges(location, ranges).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["GET_RANGES", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let time = time::Instant::now();
        let res = self.inner.head(location).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["HEAD", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let time = time::Instant::now();
        let res = self.inner.delete(location).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["DELETE", "200"])
            .observe(elapsed);
        Ok(res)
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
        let res = self.inner.list_with_delimiter(prefix).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["LIST_DELIM", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let time = time::Instant::now();
        let res = self.inner.copy(from, to).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["COPY", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn rename(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let time = time::Instant::now();
        let res = self.inner.rename(from, to).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["RENAME", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let time = time::Instant::now();
        let res = self.inner.copy_if_not_exists(from, to).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["COPY_IF", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let time = time::Instant::now();
        let res = self.inner.rename_if_not_exists(from, to).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["RENAME_IF", "200"])
            .observe(elapsed);
        Ok(res)
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
                QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&self.labels)
                    .observe(self.time.elapsed().as_secs_f64());
                t
            }
            t => t,
        }
    }
}
