super::document_metastore::document_metastore!(
    ObjectStoreMetastore,
    dyn crate::storage::ObjectStorage,
    true,
    {
        async fn check_health(&self, tenant_id: &Option<String>) -> Result<(), MetastoreError> {
            Ok(self.storage.check(tenant_id).await?)
        }

        async fn read_latency(&self, tenant_id: &Option<String>) -> std::time::Duration {
            self.storage.get_latency(tenant_id).await
        }

        async fn stream_exists(
            &self,
            stream_name: &str,
            tenant_id: &Option<String>,
        ) -> Result<bool, MetastoreError> {
            Ok(self.storage.stream_exists(stream_name, tenant_id).await?)
        }

        async fn is_stream_deleting(
            &self,
            stream_name: &str,
            tenant_id: &Option<String>,
        ) -> Result<bool, MetastoreError> {
            Ok(crate::storage::object_storage::is_tombstoned(
                self.storage.as_ref(),
                stream_name,
                tenant_id,
            )
            .await?)
        }

        async fn delete_stream(
            &self,
            stream_name: &str,
            tenant_id: &Option<String>,
        ) -> Result<(), MetastoreError> {
            Ok(self.storage.delete_stream(stream_name, tenant_id).await?)
        }

        async fn delete_stream_partition(
            &self,
            stream_name: &str,
            date: &str,
            tenant_id: &Option<String>,
        ) -> Result<(), MetastoreError> {
            let path =
                crate::storage::object_storage::stream_data_path(stream_name, &[date], tenant_id);
            Ok(self.storage.delete_prefix(&path, tenant_id).await?)
        }

        async fn list_tenant_candidates(&self) -> Result<Vec<String>, MetastoreError> {
            Ok(self
                .storage
                .list_dirs_relative(&RelativePathBuf::from_iter([""]), &None)
                .await?
                .into_iter()
                .filter(|d| !d.starts_with('.'))
                .collect())
        }
    }
);
#[cfg(test)]
mod boundary_compatibility_tests {
    use super::*;
    use crate::storage::{LocalFS, ObjectStorage, object_storage::tombstone_path};

    #[tokio::test]
    async fn partition_deletion_preserves_tenant_scope_and_adjacent_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalFS::new(dir.path().to_owned()));
        let metastore = ObjectStoreMetastore {
            storage: storage.clone(),
        };
        let tenant = Some("tenant".to_owned());
        let keys = [
            "tenant/logs/date=2026-01-01/hour=00/data.parquet",
            "tenant/logs/date=2026-01-01/ingestor.node.manifest.json",
            "tenant/logs/date=2026-01-02/ingestor.node.manifest.json",
            "logs/date=2026-01-01/ingestor.node.manifest.json",
        ];
        for key in keys {
            storage
                .put_object(
                    RelativePath::new(key),
                    Bytes::from_static(b"original bytes"),
                    &tenant,
                )
                .await
                .unwrap();
        }
        metastore
            .delete_stream_partition("logs", "date=2026-01-01", &tenant)
            .await
            .unwrap();
        for key in &keys[..2] {
            assert!(
                storage
                    .get_object(RelativePath::new(key), &tenant)
                    .await
                    .is_err()
            );
        }
        for key in &keys[2..] {
            assert_eq!(
                storage
                    .get_object(RelativePath::new(key), &tenant)
                    .await
                    .unwrap(),
                Bytes::from_static(b"original bytes")
            );
        }
    }

    #[tokio::test]
    async fn lifecycle_seams_preserve_localfs_results_and_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalFS::new(dir.path().to_owned()));
        let metastore = ObjectStoreMetastore {
            storage: storage.clone(),
        };
        assert_eq!(
            metastore.check_health(&None).await.is_ok(),
            storage.check(&None).await.is_ok()
        );
        metastore.read_latency(&None).await;
        assert!(!metastore.stream_exists("logs", &None).await.unwrap());
        assert!(!metastore.is_stream_deleting("logs", &None).await.unwrap());
        let key = RelativePath::new("logs/.stream/.stream.json");
        let bytes = Bytes::from_static(b"{\"version\":\"v7\",\"unknown\":true}");
        storage.put_object(key, bytes.clone(), &None).await.unwrap();
        assert_eq!(
            metastore.stream_exists("logs", &None).await.unwrap(),
            storage.stream_exists("logs", &None).await.unwrap()
        );
        // Discovery/existence probes never normalize or rewrite stored JSON.
        assert_eq!(storage.get_object(key, &None).await.unwrap(), bytes);
        storage
            .put_object(
                &tombstone_path("logs", &None),
                Bytes::from_static(b"null"),
                &None,
            )
            .await
            .unwrap();
        assert!(metastore.is_stream_deleting("logs", &None).await.unwrap());
        let mut expected: Vec<_> = storage
            .list_dirs_relative(RelativePath::new(""), &None)
            .await
            .unwrap()
            .into_iter()
            .filter(|d| !d.starts_with('.'))
            .collect();
        let mut actual = metastore.list_tenant_candidates().await.unwrap();
        expected.sort();
        actual.sort();
        assert_eq!(actual, expected);
        metastore.delete_stream("logs", &None).await.unwrap();
        assert!(!metastore.stream_exists("logs", &None).await.unwrap());
        // Historical prefix deletion leaves the separate marker untouched.
        assert!(metastore.is_stream_deleting("logs", &None).await.unwrap());
        let raw_error = storage
            .delete_stream("missing", &None)
            .await
            .unwrap_err()
            .to_string();
        let trait_error = metastore.delete_stream("missing", &None).await.unwrap_err();
        assert!(
            matches!(trait_error, MetastoreError::ObjectStorageError(error) if error.to_string() == raw_error)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalFS, ObjectStorage};

    #[tokio::test]
    async fn dataset_probes_are_bounded_and_fail_atomically() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for fail in [false, true] {
            let active = AtomicUsize::new(0);
            let peak = AtomicUsize::new(0);
            let result =
                super::super::document_metastore::collect_dataset_probes((0..64).map(|i| {
                    let active = &active;
                    let peak = &peak;
                    async move {
                        let n = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(n, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        if fail && i == 25 {
                            Err("failed probe")
                        } else {
                            Ok(i)
                        }
                    }
                }))
                .await;
            assert_eq!(peak.load(Ordering::SeqCst), 16);
            if fail {
                assert_eq!(result, Err("failed probe"));
            } else {
                assert_eq!(result.unwrap().len(), 64);
            }
        }
    }

    #[tokio::test]
    async fn next_metadata_read_sees_external_writer_changes() {
        for tenant in [None, Some("tenant".to_owned())] {
            let dir = tempfile::tempdir_in(".").unwrap();
            let storage = Arc::new(LocalFS::new(dir.path().to_owned()));
            let reader = ObjectStoreMetastore {
                storage: storage.clone(),
            };
            let stream_path = RelativePathBuf::from_iter([
                tenant.as_deref().unwrap_or(""),
                "metrics",
                STREAM_ROOT_DIRECTORY,
                STREAM_METADATA_FILE_NAME,
            ]);
            let manifest_path = RelativePathBuf::from_iter([
                tenant.as_deref().unwrap_or(""),
                "metrics",
                "day",
                "manifest.json",
            ]);
            for generation in ["first", "external-update"] {
                // Write directly through storage, bypassing this reader's API.
                let format = crate::storage::ObjectStoreFormat {
                    created_at: generation.into(),
                    ..Default::default()
                };
                storage
                    .put_object(&stream_path, to_bytes(&format), &tenant)
                    .await
                    .unwrap();
                let loaded: crate::storage::ObjectStoreFormat = serde_json::from_slice(
                    &reader
                        .get_stream_json("metrics", true, &tenant, false)
                        .await
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(loaded.created_at, generation);
                let manifest = Manifest {
                    version: generation.into(),
                    ..Default::default()
                };
                storage
                    .put_object(
                        &manifest_path,
                        crate::catalog::manifest::encode_manifest(&manifest).unwrap(),
                        &tenant,
                    )
                    .await
                    .unwrap();
                let loaded = reader
                    .get_manifest(
                        "metrics",
                        Utc::now(),
                        Utc::now(),
                        Some(manifest_path.to_string()),
                        &tenant,
                    )
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(loaded.version, generation);
            }
            storage
                .delete_object(&manifest_path, &tenant)
                .await
                .unwrap();
            assert!(
                reader
                    .get_manifest(
                        "metrics",
                        Utc::now(),
                        Utc::now(),
                        Some(manifest_path.to_string()),
                        &tenant
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn remote_schema_refresh_publishes_only_changes() {
        use crate::{event::commit_schema_to_metadata, metadata::LogStreamMetadata};
        use arrow_schema::{DataType, Field};
        for tenant in [None, Some("tenant".to_owned())] {
            let dir = tempfile::tempdir().unwrap();
            let storage = Arc::new(LocalFS::new(dir.path().to_owned()));
            let store = ObjectStoreMetastore {
                storage: storage.clone(),
            };
            let prefix = RelativePathBuf::from_iter([
                tenant.as_deref().unwrap_or(""),
                "logs",
                STREAM_ROOT_DIRECTORY,
            ]);
            let canonical_path = prefix.join(".schema");
            let node_path = prefix.join(".ingestor.node.schema");
            let original = Schema::new(vec![Field::new("local", DataType::Null, true)]);
            storage
                .put_object(&canonical_path, to_bytes(&original), &tenant)
                .await
                .unwrap();
            let remote = Schema::new(vec![
                Field::new("remote", DataType::Utf8, true),
                Field::new("local", DataType::Int64, true),
            ]);
            storage
                .put_object(&node_path, to_bytes(&remote), &tenant)
                .await
                .unwrap();
            let memory = std::sync::RwLock::new(LogStreamMetadata::default());
            let refreshed = store
                .refresh_schema("logs", &tenant, ".schema")
                .await
                .unwrap();
            commit_schema_to_metadata(&memory, &refreshed).unwrap();
            assert_eq!(
                memory.read().unwrap().schema["local"].data_type(),
                &DataType::Int64
            );
            assert!(memory.read().unwrap().schema.contains_key("remote"));
            let published: Schema = serde_json::from_slice(
                &storage.get_object(&canonical_path, &tenant).await.unwrap(),
            )
            .unwrap();
            assert_eq!(published, refreshed);

            // A refresh of the same union must not write, even when the files
            // present their fields in a different order.
            let file = std::fs::File::open(canonical_path.to_path(dir.path())).unwrap();
            file.set_modified(std::time::SystemTime::UNIX_EPOCH)
                .unwrap();
            store
                .refresh_schema("logs", &tenant, ".schema")
                .await
                .unwrap();
            assert_eq!(
                file.metadata().unwrap().modified().unwrap(),
                std::time::SystemTime::UNIX_EPOCH
            );
            assert_eq!(
                std::fs::metadata(canonical_path.to_path(dir.path()))
                    .unwrap()
                    .modified()
                    .unwrap(),
                std::time::SystemTime::UNIX_EPOCH
            );

            let remote = Schema::try_merge([
                remote,
                Schema::new(vec![Field::new(
                    "new_on_other_node",
                    DataType::Boolean,
                    true,
                )]),
            ])
            .unwrap();
            storage
                .put_object(&node_path, to_bytes(&remote), &tenant)
                .await
                .unwrap();
            let refreshed = store
                .refresh_schema("logs", &tenant, ".schema")
                .await
                .unwrap();
            commit_schema_to_metadata(&memory, &refreshed).unwrap();
            assert!(
                memory
                    .read()
                    .unwrap()
                    .schema
                    .contains_key("new_on_other_node")
            );
        }
    }

    #[tokio::test]
    async fn ingestor_refresh_preserves_its_schema_destination() {
        use arrow_schema::{DataType, Field};
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalFS::new(dir.path().to_owned()));
        let store = ObjectStoreMetastore {
            storage: storage.clone(),
        };
        let global = RelativePathBuf::from("logs/.stream/.schema");
        let own = RelativePathBuf::from("logs/.stream/.ingestor.own.schema");
        let remote = RelativePathBuf::from("logs/.stream/.ingestor.other.schema");
        storage
            .put_object(&global, to_bytes(&Schema::empty()), &None)
            .await
            .unwrap();
        storage
            .put_object(&own, to_bytes(&Schema::empty()), &None)
            .await
            .unwrap();
        let added = Schema::new(vec![Field::new("remote", DataType::Utf8, true)]);
        storage
            .put_object(&remote, to_bytes(&added), &None)
            .await
            .unwrap();
        store
            .refresh_schema("logs", &None, ".ingestor.own.schema")
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Schema>(&storage.get_object(&own, &None).await.unwrap())
                .unwrap(),
            added
        );
        assert_eq!(
            serde_json::from_slice::<Schema>(&storage.get_object(&global, &None).await.unwrap())
                .unwrap(),
            Schema::empty()
        );
    }

    #[derive(serde::Serialize)]
    struct TestMetastoreObject {
        id: Ulid,
        path: String,
    }

    impl MetastoreObject for TestMetastoreObject {
        fn get_object_path(&self) -> String {
            self.path.clone()
        }

        fn get_object_id(&self) -> String {
            self.id.to_string()
        }
    }

    fn saved_sql(owner: &str, id: &str, stream: &str, tenant: &Option<String>) -> Filter {
        serde_json::from_value(serde_json::json!({
            "version": "v2", "user_id": owner, "filter_id": id,
            "stream_name": stream, "filter_name": "Revenue", "tenant_id": tenant,
            "query": {"filter_type": "sql", "filter_query": "select * from orders", "filter_builder": null},
            "time_filter": null, "explorer_state": {"columns": ["amount"]},
            "future_metadata": {"preserve": true}
        })).unwrap()
    }

    #[tokio::test]
    async fn saved_filter_conversion_persists_and_delete_cannot_resurrect() {
        for tenant in [None, Some("tenant-a".to_owned())] {
            let temp = tempfile::tempdir().unwrap();
            let storage = Arc::new(LocalFS::new(temp.path().to_path_buf()));
            let store = ObjectStoreMetastore {
                storage: storage.clone(),
            };
            let legacy = saved_sql("owner", "same-id", "orders", &tenant);
            store.put_filter(&legacy, &tenant).await.unwrap();
            // Sharing an ID with a different owner or tenant cannot remove their filter.
            let other_owner = saved_sql("other-owner", "same-id", "orders", &tenant);
            store.put_filter(&other_owner, &tenant).await.unwrap();
            let mut other_id = saved_sql("owner", "different-id", "orders", &tenant);
            other_id.query.filter_type = FilterType::Search;
            store.put_filter(&other_id, &tenant).await.unwrap();
            let other_tenant = saved_sql("owner", "same-id", "orders", &Some("tenant-b".into()));
            store
                .put_filter(&other_tenant, &other_tenant.tenant_id)
                .await
                .unwrap();
            let mut canonical = legacy.clone();
            canonical.stream_name = "sql".into();
            for _ in 0..2 {
                store.put_filter(&canonical, &tenant).await.unwrap();
                store.reconcile_filter(&canonical, &tenant).await.unwrap();
                let bytes = storage
                    .get_object(&RelativePathBuf::from(canonical.get_object_path()), &tenant)
                    .await
                    .unwrap();
                assert_eq!(serde_json::from_slice::<Filter>(&bytes).unwrap(), canonical);
            }
            assert!(
                storage
                    .get_object(&RelativePathBuf::from(legacy.get_object_path()), &tenant)
                    .await
                    .is_err()
            );
            let restarted = ObjectStoreMetastore {
                storage: storage.clone(),
            };
            let loaded = restarted.load_tenant_filters(&tenant).await.unwrap();
            assert_eq!(loaded.len(), 3);
            assert!(loaded.contains(&canonical));
            // Simulate a stale legacy alias left by an interrupted earlier conversion.
            restarted.put_filter(&legacy, &tenant).await.unwrap();
            restarted.delete_filter(&canonical, &tenant).await.unwrap();
            restarted.delete_filter(&canonical, &tenant).await.unwrap();
            let loaded = restarted.load_tenant_filters(&tenant).await.unwrap();
            assert_eq!(loaded.len(), 2);
            assert!(loaded.contains(&other_owner));
            assert!(loaded.contains(&other_id));
            assert_eq!(
                restarted
                    .load_tenant_filters(&other_tenant.tenant_id)
                    .await
                    .unwrap(),
                vec![other_tenant]
            );
        }
    }

    #[tokio::test]
    async fn saved_filter_startup_prefers_sql_and_keeps_untouched_legacy() {
        for sql_first in [true, false] {
            let temp = tempfile::tempdir().unwrap();
            let storage = Arc::new(LocalFS::new(temp.path().to_path_buf()));
            let store = ObjectStoreMetastore {
                storage: storage.clone(),
            };
            let tenant = None;
            let old = saved_sql("owner", "converted", "orders", &tenant);
            let mut canonical = old.clone();
            canonical.stream_name = "sql".into();
            canonical.filter_name = "Latest name".into();
            let untouched = saved_sql("owner", "untouched", "orders", &tenant);
            for filter in if sql_first {
                [&canonical, &old]
            } else {
                [&old, &canonical]
            } {
                store.put_filter(filter, &tenant).await.unwrap();
            }
            store.put_filter(&untouched, &tenant).await.unwrap();
            let loaded = store.load_tenant_filters(&tenant).await.unwrap();
            assert_eq!(loaded.len(), 2);
            assert!(loaded.contains(&canonical));
            assert!(loaded.contains(&untouched));
            assert!(
                storage
                    .get_object(&RelativePathBuf::from(old.get_object_path()), &tenant)
                    .await
                    .is_err()
            );
            assert_eq!(store.load_tenant_filters(&tenant).await.unwrap(), loaded);
        }
    }

    #[tokio::test]
    async fn saved_filter_startup_uses_directory_tenant_for_null_payload() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalFS::new(temp.path().to_path_buf()));
        let store = ObjectStoreMetastore {
            storage: storage.clone(),
        };
        let tenant = Some("tenant-a".to_owned());
        let legacy = saved_sql("owner", "id", "orders", &tenant);
        let mut payload = serde_json::to_value(&legacy).unwrap();
        payload["tenant_id"] = serde_json::Value::Null;
        let legacy_path = RelativePathBuf::from(legacy.get_object_path());
        storage
            .put_object(&legacy_path, to_bytes(&payload), &tenant)
            .await
            .unwrap();
        let loaded = store.load_tenant_filters(&tenant).await.unwrap();
        assert_eq!(loaded, vec![legacy.clone()]);
        let mut canonical = loaded[0].clone();
        canonical.stream_name = "sql".into();
        store.put_filter(&canonical, &tenant).await.unwrap();
        store.reconcile_filter(&canonical, &tenant).await.unwrap();
        assert!(storage.get_object(&legacy_path, &tenant).await.is_err());
        assert_eq!(
            store.load_tenant_filters(&tenant).await.unwrap(),
            vec![canonical.clone()]
        );
        store.delete_filter(&canonical, &tenant).await.unwrap();
        assert!(store.load_tenant_filters(&tenant).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn saved_filter_startup_retains_non_sql_copy_with_same_identity() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalFS::new(temp.path().to_path_buf()));
        let store = ObjectStoreMetastore {
            storage: storage.clone(),
        };
        let tenant = None;
        let canonical = saved_sql("owner", "id", "sql", &tenant);
        let mut non_sql = saved_sql("owner", "id", "orders", &tenant);
        non_sql.query.filter_type = FilterType::Search;
        store.put_filter(&canonical, &tenant).await.unwrap();
        store.put_filter(&non_sql, &tenant).await.unwrap();
        for _ in 0..2 {
            assert_eq!(
                store.load_tenant_filters(&tenant).await.unwrap(),
                vec![canonical.clone()]
            );
            let bytes = storage
                .get_object(&RelativePathBuf::from(non_sql.get_object_path()), &tenant)
                .await
                .unwrap();
            assert_eq!(serde_json::from_slice::<Filter>(&bytes).unwrap(), non_sql);
        }
    }

    #[tokio::test]
    async fn saved_filter_failed_cleanup_keeps_canonical_for_retry() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalFS::new(temp.path().to_path_buf()));
        let store = ObjectStoreMetastore {
            storage: storage.clone(),
        };
        let tenant = None;
        let legacy = saved_sql("owner", "id", "orders", &tenant);
        let mut canonical = legacy.clone();
        canonical.stream_name = "sql".into();
        store.put_filter(&legacy, &tenant).await.unwrap();
        store.put_filter(&canonical, &tenant).await.unwrap();
        let legacy_path = RelativePathBuf::from(legacy.get_object_path());
        // An unreadable alias causes cleanup to fail after the canonical write.
        storage
            .put_object(&legacy_path, Bytes::from_static(b"invalid json"), &tenant)
            .await
            .unwrap();
        assert!(store.reconcile_filter(&canonical, &tenant).await.is_err());
        assert!(store.delete_filter(&canonical, &tenant).await.is_err());
        let canonical_path = RelativePathBuf::from(canonical.get_object_path());
        let bytes = storage.get_object(&canonical_path, &tenant).await.unwrap();
        assert_eq!(serde_json::from_slice::<Filter>(&bytes).unwrap(), canonical);
        // Once the transient alias failure is repaired, both retries are safe.
        store.put_filter(&legacy, &tenant).await.unwrap();
        store.reconcile_filter(&canonical, &tenant).await.unwrap();
        store.reconcile_filter(&canonical, &tenant).await.unwrap();
        assert_eq!(
            store.load_tenant_filters(&tenant).await.unwrap(),
            vec![canonical.clone()]
        );
        store.delete_filter(&canonical, &tenant).await.unwrap();
        assert!(store.load_tenant_filters(&tenant).await.unwrap().is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn saved_filter_delete_failure_reconciles_on_restart_before_delete_retry() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalFS::new(temp.path().to_path_buf()));
        let store = ObjectStoreMetastore {
            storage: storage.clone(),
        };
        let tenant = None;
        let legacy = saved_sql("owner", "id", "orders", &tenant);
        let mut canonical = legacy.clone();
        canonical.stream_name = "sql".into();
        store.put_filter(&legacy, &tenant).await.unwrap();
        store.put_filter(&canonical, &tenant).await.unwrap();
        let directory = temp.path().join(
            RelativePathBuf::from(legacy.get_object_path())
                .parent()
                .unwrap()
                .as_str(),
        );
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o555)).unwrap();
        // Root or unusual filesystems may not enforce write permissions.
        if std::fs::write(directory.join("permission-probe"), b"probe").is_ok() {
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }
        let cleanup = store.reconcile_filter(&canonical, &tenant).await;
        let deletion = store.delete_filter(&canonical, &tenant).await;
        let restart = store.load_tenant_filters(&tenant).await;
        let canonical_exists = storage
            .get_object(&RelativePathBuf::from(canonical.get_object_path()), &tenant)
            .await
            .is_ok();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(cleanup.is_err());
        assert!(deletion.is_err());
        assert!(canonical_exists);
        assert_eq!(restart.unwrap(), vec![canonical.clone()]);
        assert_eq!(
            store.load_tenant_filters(&tenant).await.unwrap(),
            vec![canonical.clone()]
        );
        assert!(
            storage
                .get_object(&RelativePathBuf::from(legacy.get_object_path()), &tenant)
                .await
                .is_err()
        );
        store.delete_filter(&canonical, &tenant).await.unwrap();
        assert!(store.load_tenant_filters(&tenant).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn saved_filter_legacy_duplicates_are_deterministic_and_not_deleted() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalFS::new(temp.path().to_path_buf()));
        let store = ObjectStoreMetastore {
            storage: storage.clone(),
        };
        let tenant = None;
        let a = saved_sql("owner", "id", "a", &tenant);
        let z = saved_sql("owner", "id", "z", &tenant);
        store.put_filter(&z, &tenant).await.unwrap();
        store.put_filter(&a, &tenant).await.unwrap();
        assert_eq!(
            store.load_tenant_filters(&tenant).await.unwrap(),
            vec![a.clone()]
        );
        assert!(
            storage
                .get_object(&RelativePathBuf::from(z.get_object_path()), &tenant)
                .await
                .is_ok()
        );
        assert!(
            storage
                .get_object(&RelativePathBuf::from(a.get_object_path()), &tenant)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn delete_alert_uses_request_tenant_path_and_is_idempotent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalFS::new(temp_dir.path().to_path_buf()));
        let metastore = ObjectStoreMetastore {
            storage: storage.clone(),
        };
        let id = Ulid::new();
        let tenant_id = None;
        let path = alert_json_path(id, &tenant_id);
        storage
            .put_object(&path, Bytes::from_static(b"{}"), &tenant_id)
            .await
            .unwrap();
        let object = TestMetastoreObject {
            id,
            path: format!("{DEFAULT_TENANT}/{ALERTS_ROOT_DIRECTORY}/{id}.json"),
        };

        metastore.delete_alert(&object, &tenant_id).await.unwrap();
        metastore.delete_alert(&object, &tenant_id).await.unwrap();

        assert!(matches!(
            storage.get_object(&path, &tenant_id).await,
            Err(ObjectStorageError::NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn delete_alert_state_uses_request_tenant_path_and_is_idempotent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalFS::new(temp_dir.path().to_path_buf()));
        let metastore = ObjectStoreMetastore {
            storage: storage.clone(),
        };
        let id = Ulid::new();
        let tenant_id = None;
        let path = alert_state_json_path(id, &tenant_id);
        storage
            .put_object(&path, Bytes::from_static(b"{}"), &tenant_id)
            .await
            .unwrap();
        let object = TestMetastoreObject {
            id,
            path: format!("{DEFAULT_TENANT}/{ALERTS_ROOT_DIRECTORY}/alert_state_{id}.json"),
        };

        metastore
            .delete_alert_state(&object, &tenant_id)
            .await
            .unwrap();
        metastore
            .delete_alert_state(&object, &tenant_id)
            .await
            .unwrap();

        assert!(matches!(
            storage.get_object(&path, &tenant_id).await,
            Err(ObjectStorageError::NoSuchKey(_))
        ));
    }
}
