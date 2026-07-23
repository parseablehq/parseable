use std::{ops::Range, path::Path as FsPath, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::execution::runtime_env::RuntimeEnv;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
    coalesce_ranges, local::LocalFileSystem, path::Path,
};

use super::metrics_layer::MetricLayer;

pub(crate) const HOT_TIER_OBJECT_STORE_URL: &str = "file://hot-tier/";
const HOT_TIER_PROVIDER: &str = "hot-tier";

pub(crate) fn hot_tier_object_path(path: &str) -> Result<Path> {
    if FsPath::new(path).is_absolute() {
        return Err(object_store::Error::Generic {
            store: HOT_TIER_PROVIDER,
            source: "hot-tier manifest path must be relative".into(),
        });
    }
    Path::parse(path).map_err(|source| object_store::Error::Generic {
        store: HOT_TIER_PROVIDER,
        source: Box::new(source),
    })
}

pub(crate) fn build_hot_tier_store(
    root: &FsPath,
    coalesce_gap: u64,
) -> Result<Arc<dyn ObjectStore>> {
    let local = LocalFileSystem::new_with_prefix(root)?;
    if coalesce_gap == 0 {
        Ok(Arc::new(MetricLayer::new(local, HOT_TIER_PROVIDER)))
    } else {
        Ok(Arc::new(MetricLayer::new(
            CoalescingStore::new(local, coalesce_gap),
            HOT_TIER_PROVIDER,
        )))
    }
}

pub(crate) fn register_hot_tier_store(
    runtime: &RuntimeEnv,
    root: &FsPath,
    coalesce_gap: u64,
) -> Result<()> {
    let store = build_hot_tier_store(root, coalesce_gap)?;
    let url = url::Url::parse(HOT_TIER_OBJECT_STORE_URL).expect("hot-tier URL is valid");
    runtime.register_object_store(&url, store);
    Ok(())
}

async fn coalesce_read<F, E, Fut>(
    ranges: &[Range<u64>],
    gap: u64,
    fetch: F,
) -> std::result::Result<Vec<Bytes>, E>
where
    F: Send + FnMut(Range<u64>) -> Fut,
    E: Send,
    Fut: std::future::Future<Output = std::result::Result<Bytes, E>> + Send,
{
    coalesce_ranges(ranges, fetch, gap).await
}

#[derive(Debug)]
struct CoalescingStore<T> {
    inner: T,
    gap: u64,
}

impl<T> CoalescingStore<T> {
    fn new(inner: T, gap: u64) -> Self {
        Self { inner, gap }
    }
}

impl<T: ObjectStore> std::fmt::Display for CoalescingStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Coalescing({})", self.inner)
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for CoalescingStore<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        coalesce_read(ranges, self.gap, |range| {
            self.inner.get_range(location, range)
        })
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        ops::Range,
        sync::{Arc, Mutex},
    };

    use bytes::Bytes;
    use datafusion::execution::{object_store::ObjectStoreUrl, runtime_env::RuntimeEnv};
    use object_store::{ObjectStoreExt, path::Path};

    use super::{
        HOT_TIER_OBJECT_STORE_URL, build_hot_tier_store, coalesce_read, hot_tier_object_path,
        register_hot_tier_store,
    };

    #[tokio::test]
    async fn dedicated_store_is_rooted_and_manifest_paths_cannot_escape() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join("stream/date=2026-07-16")).unwrap();
        std::fs::write(
            temp.path().join("stream/date=2026-07-16/a.parquet"),
            b"parquet",
        )
        .unwrap();
        let store = build_hot_tier_store(temp.path(), 0).unwrap();
        let location = hot_tier_object_path("stream/date=2026-07-16/a.parquet").unwrap();

        assert_eq!(
            store.get(&location).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"parquet")
        );
        assert!(hot_tier_object_path("../outside.parquet").is_err());
        assert!(hot_tier_object_path("/tmp/outside.parquet").is_err());
        assert_ne!(HOT_TIER_OBJECT_STORE_URL, "file:///");
    }

    #[tokio::test]
    async fn dedicated_store_registers_without_replacing_staging_store() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("a.parquet"), b"hot").unwrap();
        let runtime = RuntimeEnv::default();

        register_hot_tier_store(&runtime, temp.path(), 0).unwrap();

        let hot_url = ObjectStoreUrl::parse(HOT_TIER_OBJECT_STORE_URL).unwrap();
        let hot_store = runtime.object_store(hot_url).unwrap();
        assert_eq!(
            hot_store
                .get(&Path::from("a.parquet"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"hot")
        );
        assert!(
            runtime
                .object_store(ObjectStoreUrl::parse("file:///").unwrap())
                .is_ok()
        );
    }

    #[tokio::test]
    async fn coalescing_handles_unordered_adjacent_and_overlapping_ranges() {
        let source = Bytes::from_static(b"0123456789abcdefghij");
        let fetched = Arc::new(Mutex::new(Vec::<Range<u64>>::new()));
        let ranges = vec![10..14, 0..4, 3..7, 8..10];

        let result = coalesce_read(&ranges, 0, {
            let fetched = Arc::clone(&fetched);
            move |range| {
                fetched.lock().unwrap().push(range.clone());
                let bytes = source.slice(range.start as usize..range.end as usize);
                async move { Ok::<_, io::Error>(bytes) }
            }
        })
        .await
        .unwrap();

        assert_eq!(
            result,
            vec![
                Bytes::from_static(b"abcd"),
                Bytes::from_static(b"0123"),
                Bytes::from_static(b"3456"),
                Bytes::from_static(b"89"),
            ]
        );
        assert_eq!(*fetched.lock().unwrap(), vec![0..7, 8..14]);
    }

    #[tokio::test]
    async fn coalescing_handles_empty_and_propagates_failures() {
        let empty = coalesce_read(&[], 1024, |_range| async {
            Ok::<_, io::Error>(Bytes::new())
        })
        .await
        .unwrap();
        assert!(empty.is_empty());

        let error = coalesce_read(&[0..2, 4..6], 0, |_range| async {
            Err::<Bytes, _>(io::Error::other("read failed"))
        })
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
    }
}
