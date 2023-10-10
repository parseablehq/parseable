use std::{
    ops::Range,
    task::{Context, Poll},
    time,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{stream::BoxStream, Stream, StreamExt};
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
};
use tokio::io::AsyncWrite;

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
    async fn put(&self, location: &Path, bytes: Bytes) -> object_store::Result<()> {
        let time = time::Instant::now();
        self.inner.put(location, bytes).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT", "200"])
            .observe(elapsed);
        return Ok(());
    }

    // todo completly tracking multipart upload
    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let time = time::Instant::now();
        let (id, write) = self.inner.put_multipart(location).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT_MULTIPART", "200"])
            .observe(elapsed);

        Ok((id, write))
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> object_store::Result<()> {
        let time = time::Instant::now();
        let elapsed = time.elapsed().as_secs_f64();
        self.inner.abort_multipart(location, multipart_id).await?;
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT_MULTIPART_ABORT", "200"])
            .observe(elapsed);
        Ok(())
    }

    async fn append(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn AsyncWrite + Unpin + Send>> {
        let time = time::Instant::now();
        let write = self.inner.append(location).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["APPEND", "200"])
            .observe(elapsed);

        Ok(write)
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        let time = time::Instant::now();
        let res = self.inner.get(location).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let time = time::Instant::now();
        let res = self.inner.get_opts(location, options).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["GET_OPTS", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
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
    ) -> object_store::Result<Vec<Bytes>> {
        let time = time::Instant::now();
        let res = self.inner.get_ranges(location, ranges).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["GET_RANGES", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let time = time::Instant::now();
        let res = self.inner.head(location).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["HEAD", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
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
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        let time = time::Instant::now();
        let inner = self.inner.list(prefix).await?;
        let res = StreamMetricWrapper {
            time,
            labels: ["LIST", "200"],
            inner,
        };
        Ok(Box::pin(res))
    }

    async fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        let time = time::Instant::now();
        let inner = self.inner.list_with_offset(prefix, offset).await?;
        let res = StreamMetricWrapper {
            time,
            labels: ["LIST_OFFSET", "200"],
            inner,
        };
        Ok(Box::pin(res))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let time = time::Instant::now();
        let res = self.inner.list_with_delimiter(prefix).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["LIST_DELIM", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let time = time::Instant::now();
        let res = self.inner.copy(from, to).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["COPY", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let time = time::Instant::now();
        let res = self.inner.rename(from, to).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["RENAME", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let time = time::Instant::now();
        let res = self.inner.copy_if_not_exists(from, to).await?;
        let elapsed = time.elapsed().as_secs_f64();
        QUERY_LAYER_STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["COPY_IF", "200"])
            .observe(elapsed);
        Ok(res)
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
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
