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
 */

//! Append-only, per-stream canonical schema resolution for distributed ingest.
//!
//! Canonical fields are keyed by their published name. Once a name has been
//! published, its datatype is immutable; a conflicting local field is given a
//! deterministic datatype suffix instead. Resolutions only replace schema
//! metadata and retain the original Arrow arrays.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema};
use bytes::Bytes;
use tonic::async_trait;

use crate::{
    event::format::get_datatype_suffix,
    metastore::{MetastoreError, metastore_traits::Metastore},
    option::Mode,
    parseable::{GlobalSchema, PARSEABLE},
};

const MAX_CAS_ATTEMPTS: usize = 16;
const MAX_NAME_ATTEMPTS: usize = 64;

#[async_trait]
trait RegistryBackend: Send + Sync {
    async fn read_global(&self) -> Result<Option<GlobalSchema>, MetastoreError>;
    async fn read_legacy_schemas(&self) -> Result<Vec<Schema>, MetastoreError>;
    async fn compare_and_swap(
        &self,
        schema: Schema,
        expected: Option<object_store::UpdateVersion>,
    ) -> Result<bool, MetastoreError>;
}

struct MetastoreRegistryBackend<'a> {
    metastore: &'a dyn Metastore,
    stream_name: &'a str,
    tenant_id: &'a Option<String>,
}

#[async_trait]
impl RegistryBackend for MetastoreRegistryBackend<'_> {
    async fn read_global(&self) -> Result<Option<GlobalSchema>, MetastoreError> {
        self.metastore
            .get_global_schema(self.stream_name, self.tenant_id)
            .await
    }

    async fn read_legacy_schemas(&self) -> Result<Vec<Schema>, MetastoreError> {
        self.metastore
            .get_all_schemas(self.stream_name, self.tenant_id)
            .await
    }

    async fn compare_and_swap(
        &self,
        schema: Schema,
        expected: Option<object_store::UpdateVersion>,
    ) -> Result<bool, MetastoreError> {
        self.metastore
            .put_global_schema(schema, self.stream_name, self.tenant_id, expected)
            .await
    }
}

/// Maps each local `(field name, datatype)` to its canonical Arrow field.
#[derive(Debug, Clone)]
pub struct Resolution {
    targets: HashMap<(String, DataType), FieldRef>,
    identity: bool,
}

impl Default for Resolution {
    fn default() -> Self {
        Self::identity()
    }
}

impl Resolution {
    /// A passthrough resolution. The original schema and batch are retained.
    pub fn identity() -> Self {
        Self {
            targets: HashMap::new(),
            identity: true,
        }
    }

    /// Construct a resolution from the mapping maintained by the stream layer.
    pub fn from_targets(targets: HashMap<(String, DataType), FieldRef>) -> Self {
        let targets = targets
            .into_iter()
            .map(|(key, field)| {
                (
                    key,
                    Arc::new(Field::new(field.name(), field.data_type().clone(), true)),
                )
            })
            .collect();
        Self {
            targets,
            identity: false,
        }
    }

    pub fn is_identity(&self) -> bool {
        self.identity
    }

    /// Find the canonical target for a local field.
    pub fn target(&self, name: &str, data_type: &DataType) -> Option<&FieldRef> {
        self.targets.get(&(name.to_owned(), data_type.clone()))
    }

    /// Retain already published columns in the cached in-memory view. Canonical
    /// metadata is not a raw local schema and must not enter the local merge.
    pub(crate) fn include_identity_schema(&mut self, schema: &Schema) {
        for field in schema
            .fields()
            .iter()
            .filter(|field| !field.data_type().is_null())
        {
            self.targets
                .entry((field.name().clone(), field.data_type().clone()))
                .or_insert_with(|| {
                    Arc::new(Field::new(field.name(), field.data_type().clone(), true))
                });
        }
    }

    /// Resolve a schema without touching its arrays.
    pub fn rename_schema(&self, schema: &Schema) -> Result<Schema, ArrowError> {
        if self.identity {
            return Ok(schema.clone());
        }

        let mut fields = Vec::with_capacity(schema.fields().len());
        let mut target_names = HashSet::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let Some(target) = self.target_field(field)? else {
                // Top-level Null fields carry no values and may be omitted.
                continue;
            };
            if !target_names.insert(target.name().clone()) {
                return Err(ArrowError::SchemaError(format!(
                    "multiple source columns resolve to canonical field '{}'",
                    target.name()
                )));
            }
            fields.push(target);
        }
        Ok(Schema::new(fields))
    }

    /// Rename field metadata while retaining every non-Null array by Arc clone.
    pub fn rename_batch(&self, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        if self.identity {
            return Ok(batch.clone());
        }

        let mut fields = Vec::with_capacity(batch.num_columns());
        let mut columns = Vec::with_capacity(batch.num_columns());
        let mut target_names = HashSet::with_capacity(batch.num_columns());
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            let Some(target) = self.target_field(field)? else {
                continue;
            };
            if !target_names.insert(target.name().clone()) {
                return Err(ArrowError::SchemaError(format!(
                    "multiple source columns resolve to canonical field '{}'",
                    target.name()
                )));
            }
            fields.push(target);
            columns.push(Arc::clone(column));
        }

        RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
    }

    fn target_field(&self, field: &Field) -> Result<Option<FieldRef>, ArrowError> {
        if field.data_type().is_null() {
            return Ok(None);
        }
        self.target(field.name(), field.data_type())
            .filter(|target| target.data_type() == field.data_type())
            .cloned()
            .map(Some)
            .ok_or_else(|| {
                ArrowError::SchemaError(format!(
                    "no canonical resolution for field '{}' of type {}",
                    field.name(),
                    field.data_type()
                ))
            })
    }
}

/// Whether staging may bootstrap a new global canonical schema registry.
///
/// Only the explicit value `false` disables bootstrap; malformed values do not
/// silently disable it. Existing coordinated streams must keep resolving against
/// their registry even when bootstrap is disabled, to preserve published names.
/// The caller remains responsible for applying this only to the staging path.
pub fn enabled() -> bool {
    PARSEABLE.options.mode == Mode::Ingest
        && !std::env::var("P_CANONICAL_SCHEMA")
            .map(|value| value.trim().eq_ignore_ascii_case("false"))
            .unwrap_or(false)
}

/// Resolve all schemas in one operation against an append-only global schema.
///
/// On every compare-and-swap contention, the global schema is re-read and the
/// complete local set is resolved again. No mapping computed against a losing
/// snapshot is returned.
pub async fn resolve(
    metastore: &dyn Metastore,
    stream_name: &str,
    tenant_id: &Option<String>,
    schemas: &[Schema],
    pinned: &HashSet<String>,
) -> Result<Resolution, MetastoreError> {
    let backend = MetastoreRegistryBackend {
        metastore,
        stream_name,
        tenant_id,
    };
    resolve_backend(&backend, stream_name, schemas, pinned).await
}

async fn resolve_backend(
    backend: &dyn RegistryBackend,
    stream_name: &str,
    schemas: &[Schema],
    pinned: &HashSet<String>,
) -> Result<Resolution, MetastoreError> {
    for attempt in 0..MAX_CAS_ATTEMPTS {
        let global = backend.read_global().await?;
        let (canonical, original_schema) = match global.as_ref() {
            Some(global) => {
                let original = parse_global_schema(&global.schema)?;
                let normalized = normalize_registry(&original).map_err(schema_error)?;
                (normalized, Some(original))
            }
            None => {
                // Legacy schemas are consulted only while bootstrapping an
                // absent global registry. Conflicts are handled leniently.
                let legacy = backend.read_legacy_schemas().await?;
                (legacy_seed(&legacy), None)
            }
        };

        let (resolution, proposed) =
            build_resolution(&canonical, schemas, pinned).map_err(schema_error)?;
        let needs_publish = match (&original_schema, global.is_some()) {
            (Some(original), true) => original != &proposed,
            (None, false) => !proposed.fields().is_empty(),
            _ => true,
        };

        if !needs_publish {
            return Ok(resolution);
        }

        // `None` is explicitly create-only; a snapshot version is an
        // update-only CAS. A failed CAS invalidates this whole resolution.
        let expected = global.as_ref().map(|snapshot| snapshot.version.clone());
        if backend.compare_and_swap(proposed, expected).await? {
            return Ok(resolution);
        }

        if attempt + 1 < MAX_CAS_ATTEMPTS {
            let backoff_ms = 2_u64.saturating_mul(1_u64 << attempt.min(6));
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    Err(schema_error(format!(
        "global schema compare-and-swap did not converge after {MAX_CAS_ATTEMPTS} attempts for stream '{stream_name}'"
    )))
}

fn parse_global_schema(bytes: &Bytes) -> Result<Schema, MetastoreError> {
    serde_json::from_slice(bytes).map_err(MetastoreError::from)
}

fn schema_error(message: impl Into<String>) -> MetastoreError {
    MetastoreError::JsonSchemaError {
        message: message.into(),
    }
}

/// Make a field match the registry's canonical representation.
fn canonical_field(field: &Field) -> FieldRef {
    Arc::new(Field::new(field.name(), field.data_type().clone(), true))
}

/// Normalize a persisted registry and reject entries that violate its identity
/// rules. Existing `(name, datatype)` claims are never rewritten to a new type.
fn normalize_registry(schema: &Schema) -> Result<Schema, String> {
    let mut fields = BTreeMap::<String, FieldRef>::new();
    for field in schema.fields() {
        if field.data_type().is_null() {
            return Err(format!(
                "global schema contains unsupported Null field '{}'",
                field.name()
            ));
        }
        if fields
            .insert(field.name().clone(), canonical_field(field))
            .is_some()
        {
            return Err(format!(
                "global schema contains duplicate field name '{}'",
                field.name()
            ));
        }
    }
    Ok(Schema::new(fields.into_values().collect::<Vec<_>>()))
}

/// Build a stable, tolerant union of the old per-node schemas for bootstrapping.
fn legacy_seed(schemas: &[Schema]) -> Schema {
    let mut chosen = BTreeMap::<String, FieldRef>::new();
    for field in schemas.iter().flat_map(|schema| schema.fields()) {
        if field.data_type().is_null() {
            continue;
        }
        let replace = chosen
            .get(field.name())
            .map(|current| legacy_type_precedes(field.data_type(), current.data_type()))
            .unwrap_or(true);
        if replace {
            chosen.insert(field.name().clone(), canonical_field(field));
        }
    }
    Schema::new(chosen.into_values().collect::<Vec<_>>())
}

fn legacy_type_precedes(candidate: &DataType, current: &DataType) -> bool {
    match (is_utf8_family(candidate), is_utf8_family(current)) {
        (true, false) => true,
        (false, true) => false,
        _ => format!("{candidate:?}") < format!("{current:?}"),
    }
}

fn is_utf8_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Resolve a set of schemas against the canonical snapshot and return the
/// resulting canonical schema. This is intentionally storage-independent.
fn build_resolution(
    canonical: &Schema,
    schemas: &[Schema],
    pinned: &HashSet<String>,
) -> Result<(Resolution, Schema), String> {
    let normalized = normalize_registry(canonical)?;
    let mut fields: BTreeMap<String, FieldRef> = normalized
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.clone()))
        .collect();

    let mut keys = HashSet::<(String, DataType)>::new();
    for schema in schemas {
        for field in schema.fields() {
            if !field.data_type().is_null() {
                keys.insert((field.name().clone(), field.data_type().clone()));
            }
        }
    }
    let mut keys: Vec<_> = keys.into_iter().collect();
    keys.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| format!("{:?}", left.1).cmp(&format!("{:?}", right.1)))
    });

    // Record only names of distinct columns that co-occur with each exact
    // source key. Schemas that are processed separately must not reserve names
    // from one another, since compatible canonical suffix fields are reusable.
    let mut cooccurring_source_names = HashMap::<(String, DataType), HashSet<String>>::new();
    for schema in schemas {
        let fields: Vec<_> = schema
            .fields()
            .iter()
            .filter(|field| !field.data_type().is_null())
            .collect();
        for (index, field) in fields.iter().enumerate() {
            let key = (field.name().clone(), field.data_type().clone());
            let other_names = cooccurring_source_names.entry(key).or_default();
            for (other_index, other) in fields.iter().enumerate() {
                if index != other_index {
                    other_names.insert(other.name().clone());
                }
            }
        }
    }

    // Reserve free source names first so conflict suffixes do not capture
    // fields that require their original canonical names.
    let mut by_name = BTreeMap::<String, Vec<DataType>>::new();
    for (name, data_type) in &keys {
        by_name
            .entry(name.clone())
            .or_default()
            .push(data_type.clone());
    }
    for (name, types) in &by_name {
        if fields.contains_key(name) {
            continue;
        }
        if pinned.contains(name) && types.len() > 1 {
            return Err(format!(
                "pinned field '{name}' has incompatible local datatypes"
            ));
        }
        if let Some(data_type) = types.first() {
            fields.insert(
                name.clone(),
                Arc::new(Field::new(name, data_type.clone(), true)),
            );
        }
    }

    let mut targets = HashMap::with_capacity(keys.len());
    for (name, data_type) in &keys {
        let direct_match = fields
            .get(name)
            .is_some_and(|field| field.data_type() == data_type);
        if direct_match {
            targets.insert(
                (name.clone(), data_type.clone()),
                fields.get(name).expect("checked above").clone(),
            );
            continue;
        }

        if pinned.contains(name) {
            let established_type = fields
                .get(name)
                .map(|field| field.data_type().clone())
                .unwrap_or_else(|| data_type.clone());
            return Err(format!(
                "pinned field '{name}' has local datatype {data_type} but canonical datatype {established_type}"
            ));
        }

        let suffix = get_datatype_suffix(data_type);
        let mut candidate = format!("{name}_{suffix}");
        let mut target = None;
        let source_key = (name.clone(), data_type.clone());
        for _ in 0..MAX_NAME_ATTEMPTS {
            let conflicts_with_local_column = cooccurring_source_names
                .get(&source_key)
                .is_some_and(|names| names.contains(&candidate));
            if pinned.contains(&candidate) || conflicts_with_local_column {
                candidate.push_str(&format!("_{suffix}"));
                continue;
            }
            match fields.get(&candidate) {
                Some(field) if field.data_type() == data_type => {
                    target = Some(field.clone());
                    break;
                }
                Some(_) => candidate.push_str(&format!("_{suffix}")),
                None => {
                    let field = Arc::new(Field::new(&candidate, data_type.clone(), true));
                    fields.insert(candidate.clone(), field.clone());
                    target = Some(field);
                    break;
                }
            }
        }
        let target = target.ok_or_else(|| {
            format!(
                "could not find an available canonical name for '{name}' after {MAX_NAME_ATTEMPTS} suffix attempts"
            )
        })?;
        targets.insert((name.clone(), data_type.clone()), target);
    }

    // Ensure each input schema remains lossless: every retained source column
    // must have a unique target name in that schema.
    for schema in schemas {
        let mut names = HashSet::with_capacity(schema.fields().len());
        for field in schema.fields() {
            if field.data_type().is_null() {
                continue;
            }
            let key = (field.name().clone(), field.data_type().clone());
            let target = targets.get(&key).ok_or_else(|| {
                format!(
                    "no canonical resolution for field '{}' of type {}",
                    field.name(),
                    field.data_type()
                )
            })?;
            if !names.insert(target.name().clone()) {
                return Err(format!(
                    "multiple source columns in one schema resolve to canonical field '{}'",
                    target.name()
                ));
            }
        }
    }

    let canonical = Schema::new(fields.into_values().collect::<Vec<_>>());
    Ok((Resolution::from_targets(targets), canonical))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, FieldRef, Schema, TimeUnit};
    use bytes::Bytes;
    use object_store::UpdateVersion;
    use tokio::sync::Barrier;
    use tonic::async_trait;

    use crate::{metastore::MetastoreError, parseable::GlobalSchema};

    use super::{
        MAX_CAS_ATTEMPTS, RegistryBackend, Resolution, build_resolution, canonical_field,
        legacy_seed, normalize_registry, resolve_backend,
    };

    fn field(name: &str, data_type: DataType) -> FieldRef {
        Arc::new(Field::new(name, data_type, true))
    }

    fn resolve_local(
        canonical: Schema,
        schemas: &[Schema],
        pinned: &[&str],
    ) -> Result<(Resolution, Schema), String> {
        let pinned = pinned.iter().map(|name| (*name).to_owned()).collect();
        build_resolution(&canonical, schemas, &pinned)
    }

    struct MemoryState {
        schema: Option<Schema>,
        version: u64,
        legacy: Vec<Schema>,
    }

    struct InMemoryBackend {
        state: Mutex<MemoryState>,
        initial_read_barrier: Option<Arc<Barrier>>,
        barrier_readers: usize,
        global_reads: AtomicUsize,
        legacy_reads: AtomicUsize,
        cas_attempts: AtomicUsize,
        force_contention: bool,
        fail_read: bool,
        fail_write: bool,
    }

    impl InMemoryBackend {
        fn new(schema: Option<Schema>, legacy: Vec<Schema>, barrier_readers: usize) -> Self {
            Self {
                state: Mutex::new(MemoryState {
                    version: if schema.is_some() { 1 } else { 0 },
                    schema,
                    legacy,
                }),
                initial_read_barrier: (barrier_readers > 0)
                    .then(|| Arc::new(Barrier::new(barrier_readers))),
                barrier_readers,
                global_reads: AtomicUsize::new(0),
                legacy_reads: AtomicUsize::new(0),
                cas_attempts: AtomicUsize::new(0),
                force_contention: false,
                fail_read: false,
                fail_write: false,
            }
        }

        fn with_contention(mut self) -> Self {
            self.force_contention = true;
            self
        }

        fn with_read_error(mut self) -> Self {
            self.fail_read = true;
            self
        }

        fn with_write_error(mut self) -> Self {
            self.fail_write = true;
            self
        }

        fn global_read_count(&self) -> usize {
            self.global_reads.load(Ordering::SeqCst)
        }

        fn legacy_read_count(&self) -> usize {
            self.legacy_reads.load(Ordering::SeqCst)
        }

        fn cas_attempt_count(&self) -> usize {
            self.cas_attempts.load(Ordering::SeqCst)
        }

        fn canonical_schema(&self) -> Option<Schema> {
            self.state.lock().unwrap().schema.clone()
        }
    }

    fn memory_version(version: u64) -> UpdateVersion {
        UpdateVersion {
            e_tag: Some(format!("memory-{version}")),
            version: None,
        }
    }

    fn injected_storage_error() -> MetastoreError {
        MetastoreError::Error {
            status_code: actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            message: "injected registry storage failure".to_owned(),
            flow: "schema_registry_test".to_owned(),
        }
    }

    fn is_injected_storage_error(error: &MetastoreError) -> bool {
        matches!(
            error,
            MetastoreError::Error { message, .. }
                if message == "injected registry storage failure"
        )
    }

    #[async_trait]
    impl RegistryBackend for InMemoryBackend {
        async fn read_global(&self) -> Result<Option<GlobalSchema>, MetastoreError> {
            let read_number = self.global_reads.fetch_add(1, Ordering::SeqCst);
            if self.fail_read {
                return Err(injected_storage_error());
            }

            let snapshot = {
                let state = self.state.lock().unwrap();
                state.schema.as_ref().map(|schema| {
                    let bytes = serde_json::to_vec(schema).expect("serialize in-memory schema");
                    GlobalSchema::new(Bytes::from(bytes), memory_version(state.version))
                })
            };
            if read_number < self.barrier_readers {
                self.initial_read_barrier
                    .as_ref()
                    .expect("barrier configured for the first reads")
                    .wait()
                    .await;
            }
            Ok(snapshot)
        }

        async fn read_legacy_schemas(&self) -> Result<Vec<Schema>, MetastoreError> {
            self.legacy_reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.state.lock().unwrap().legacy.clone())
        }

        async fn compare_and_swap(
            &self,
            schema: Schema,
            expected: Option<UpdateVersion>,
        ) -> Result<bool, MetastoreError> {
            self.cas_attempts.fetch_add(1, Ordering::SeqCst);
            if self.fail_write {
                return Err(injected_storage_error());
            }
            if self.force_contention {
                return Ok(false);
            }

            let mut state = self.state.lock().unwrap();
            let matches_expected = match (&state.schema, expected) {
                (None, None) => true,
                (Some(_), Some(expected)) => expected == memory_version(state.version),
                _ => false,
            };
            if !matches_expected {
                return Ok(false);
            }

            state.schema = Some(schema);
            state.version += 1;
            Ok(true)
        }
    }

    fn assert_schemas_merge_into(canonical: &Schema, resolved: impl IntoIterator<Item = Schema>) {
        let mut schemas = vec![canonical.clone()];
        schemas.extend(resolved);
        assert_eq!(Schema::try_merge(schemas).unwrap(), canonical.clone());
    }

    #[test]
    fn default_resolution_is_identity() {
        let resolution = Resolution::default();
        let schema = Schema::new(vec![field("a", DataType::Int64)]);
        assert!(resolution.is_identity());
        assert_eq!(resolution.rename_schema(&schema).unwrap(), schema);
    }

    #[test]
    fn canonical_identity_targets_preserve_local_renames() {
        let canonical = Schema::new(vec![
            field("a", DataType::Utf8),
            field("a_int64", DataType::Int64),
        ]);
        let local = Schema::new(vec![field("a", DataType::Int64)]);
        let (mut resolution, _) = resolve_local(canonical.clone(), &[local.clone()], &[]).unwrap();
        resolution.include_identity_schema(&canonical);

        assert_eq!(
            resolution.target("a", &DataType::Int64).unwrap().name(),
            "a_int64"
        );
        assert_eq!(resolution.rename_schema(&canonical).unwrap(), canonical);
        assert_eq!(
            resolution.rename_schema(&local).unwrap().field(0).name(),
            "a_int64"
        );
        assert!(resolution.target("unclaimed", &DataType::Utf8).is_none());
    }

    #[test]
    fn existing_type_is_retained_and_local_conflict_is_renamed() {
        let canonical = Schema::new(vec![field("a", DataType::Utf8)]);
        let local = [Schema::new(vec![field("a", DataType::Int64)])];
        let (resolution, updated) = resolve_local(canonical, &local, &[]).unwrap();
        assert_eq!(
            resolution.target("a", &DataType::Int64).unwrap().name(),
            "a_int64"
        );
        assert_eq!(
            updated.field_with_name("a").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn suffix_resolution_repeats_past_occupied_names() {
        let canonical = Schema::new(vec![
            field("a", DataType::Utf8),
            field("a_int64", DataType::Utf8),
        ]);
        let local = [Schema::new(vec![field("a", DataType::Int64)])];
        let (resolution, _) = resolve_local(canonical, &local, &[]).unwrap();
        assert_eq!(
            resolution.target("a", &DataType::Int64).unwrap().name(),
            "a_int64_int64"
        );
    }

    #[test]
    fn nested_datatype_equality_is_conservative() {
        let item_nullable = DataType::List(field("item", DataType::Int64));
        let item_non_nullable =
            DataType::List(Arc::new(Field::new("item", DataType::Int64, false)));
        let canonical = Schema::new(vec![field("nested", item_nullable)]);
        let local = [Schema::new(vec![field(
            "nested",
            item_non_nullable.clone(),
        )])];
        let (resolution, _) = resolve_local(canonical, &local, &[]).unwrap();
        assert_eq!(
            resolution
                .target("nested", &item_non_nullable)
                .unwrap()
                .name(),
            "nested_list"
        );
    }

    #[test]
    fn pinned_fields_fail_on_type_conflict() {
        let canonical = Schema::new(vec![field("p_timestamp", DataType::Utf8)]);
        let local = [Schema::new(vec![field("p_timestamp", DataType::Int64)])];
        assert!(resolve_local(canonical, &local, &["p_timestamp"]).is_err());
    }

    #[test]
    fn source_name_reservation_prevents_suffix_aliasing() {
        let canonical = Schema::new(vec![field("a", DataType::Utf8)]);
        let local = [Schema::new(vec![
            field("a", DataType::Int64),
            field("a_int64", DataType::Int64),
        ])];
        let (resolution, _) = resolve_local(canonical, &local, &[]).unwrap();
        assert_eq!(
            resolution.target("a", &DataType::Int64).unwrap().name(),
            "a_int64_int64"
        );
        assert_eq!(
            resolution
                .target("a_int64", &DataType::Int64)
                .unwrap()
                .name(),
            "a_int64"
        );
    }

    #[test]
    fn separate_schemas_can_reuse_compatible_canonical_suffix_name() {
        let canonical = Schema::new(vec![
            field("x", DataType::Utf8),
            field("x_int64", DataType::Int64),
        ]);
        let old_raw_schema = Schema::new(vec![field("x", DataType::Int64)]);
        let canonical_shaped_schema = Schema::new(vec![
            field("x", DataType::Utf8),
            field("x_int64", DataType::Int64),
        ]);
        let local = [old_raw_schema, canonical_shaped_schema];

        let (resolution, updated) = resolve_local(canonical.clone(), &local, &[]).unwrap();
        assert_eq!(
            resolution.target("x", &DataType::Int64).unwrap().name(),
            "x_int64"
        );
        assert_eq!(updated, canonical);
    }

    #[test]
    fn pinned_name_is_never_claimed_by_a_generated_suffix() {
        let canonical = Schema::new(vec![
            field("foo", DataType::Int64),
            field("foo_utf8", DataType::Utf8),
        ]);
        let local = [Schema::new(vec![field("foo", DataType::Utf8)])];
        let (resolution, updated) = resolve_local(canonical, &local, &["foo_utf8"]).unwrap();

        assert_eq!(
            resolution.target("foo", &DataType::Utf8).unwrap().name(),
            "foo_utf8_utf8"
        );
        assert_eq!(
            updated.field_with_name("foo_utf8").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn duplicate_target_columns_are_rejected_instead_of_dropped() {
        let target = field("a_x", DataType::Utf8);
        let resolution = Resolution::from_targets(
            [
                (("a".to_owned(), DataType::Utf8), target.clone()),
                (("a_x".to_owned(), DataType::Utf8), target),
            ]
            .into_iter()
            .collect(),
        );
        let schema = Schema::new(vec![
            field("a", DataType::Utf8),
            field("a_x", DataType::Utf8),
        ]);
        assert!(resolution.rename_schema(&schema).is_err());

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![Some("first")])),
                Arc::new(StringArray::from(vec![Some("second")])),
            ],
        )
        .unwrap();
        assert!(resolution.rename_batch(&batch).is_err());
    }

    #[test]
    fn top_level_null_columns_drop_but_empty_batch_keeps_rows() {
        let null_column = Arc::new(arrow_array::NullArray::new(4)) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field("nulls", DataType::Null)])),
            vec![null_column],
        )
        .unwrap();
        let resolution = Resolution::from_targets(HashMap::new());
        let out = resolution.rename_batch(&batch).unwrap();
        assert_eq!(out.num_rows(), 4);
        assert_eq!(out.num_columns(), 0);
        assert!(
            resolution
                .rename_schema(batch.schema().as_ref())
                .unwrap()
                .fields()
                .is_empty()
        );
    }

    #[test]
    fn renamed_batch_keeps_column_order_row_count_and_array_arcs() {
        let resolution = Resolution::from_targets(
            [
                (("b".to_owned(), DataType::Utf8), field("b", DataType::Utf8)),
                (
                    ("a".to_owned(), DataType::Int64),
                    field("a_int64", DataType::Int64),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let first: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), Some("y")]));
        let second: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                field("b", DataType::Utf8),
                field("a", DataType::Int64),
            ])),
            vec![first.clone(), second.clone()],
        )
        .unwrap();
        let out = resolution.rename_batch(&batch).unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out.schema().field(0).name(), "b");
        assert_eq!(out.schema().field(1).name(), "a_int64");
        assert!(Arc::ptr_eq(&first, out.column(0)));
        assert!(Arc::ptr_eq(&second, out.column(1)));
    }

    #[test]
    fn identity_resolution_retains_schema_metadata_and_original_fields() {
        let original = Schema::new_with_metadata(
            vec![Arc::new(
                Field::new("a", DataType::Int64, false)
                    .with_metadata([("origin".to_owned(), "source".to_owned())].into()),
            )],
            [("schema".to_owned(), "metadata".to_owned())].into(),
        );
        let renamed = Resolution::identity().rename_schema(&original).unwrap();
        assert_eq!(renamed, original);
        assert_eq!(renamed.field(0).metadata(), original.field(0).metadata());
        assert_eq!(renamed.metadata(), original.metadata());
    }

    #[test]
    fn canonical_fields_and_registry_are_normalized_and_sorted() {
        let noncanonical_target = Arc::new(
            Field::new("target", DataType::Utf8, false)
                .with_metadata([("source".to_owned(), "legacy".to_owned())].into()),
        );
        let resolution = Resolution::from_targets(
            [(("source".to_owned(), DataType::Utf8), noncanonical_target)]
                .into_iter()
                .collect(),
        );
        let target = resolution.target("source", &DataType::Utf8).unwrap();
        assert!(target.is_nullable());
        assert!(target.metadata().is_empty());

        let schema = Schema::new(vec![
            Arc::new(
                Field::new("z", DataType::Int64, false)
                    .with_metadata([("k".to_owned(), "v".to_owned())].into()),
            ),
            Arc::new(Field::new("a", DataType::Utf8, false)),
        ]);
        let normalized = normalize_registry(&schema).unwrap();
        assert_eq!(normalized.field(0).name(), "a");
        assert_eq!(normalized.field(1).name(), "z");
        assert!(normalized.fields().iter().all(|field| field.is_nullable()));
        assert!(
            normalized
                .fields()
                .iter()
                .all(|field| field.metadata().is_empty())
        );
        assert!(normalized.metadata().is_empty());
        assert_eq!(canonical_field(schema.field(1)), field("a", DataType::Utf8));
    }

    #[test]
    fn canonical_registry_rejects_duplicate_names_and_null_fields() {
        let duplicates = Schema::new(vec![field("a", DataType::Utf8), field("a", DataType::Utf8)]);
        assert!(normalize_registry(&duplicates).is_err());
        let nulls = Schema::new(vec![field("n", DataType::Null)]);
        assert!(normalize_registry(&nulls).is_err());
    }

    #[test]
    fn legacy_union_is_stable_and_prefers_utf8_for_conflicts() {
        let first = Schema::new(vec![
            field("x", DataType::Int64),
            field("b", DataType::Boolean),
        ]);
        let second = Schema::new(vec![field("x", DataType::Utf8)]);
        let forward = legacy_seed(&[first.clone(), second.clone()]);
        let reverse = legacy_seed(&[second, first]);
        assert_eq!(forward, reverse);
        assert_eq!(
            forward.field_with_name("x").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(forward.field(0).name(), "b");
    }

    #[test]
    fn resolution_is_deterministic_across_schema_order() {
        let a = Schema::new(vec![field("value", DataType::Int64)]);
        let b = Schema::new(vec![field("value", DataType::Utf8)]);
        let (first, first_schema) =
            resolve_local(Schema::empty(), &[a.clone(), b.clone()], &[]).unwrap();
        let (second, second_schema) = resolve_local(Schema::empty(), &[b, a], &[]).unwrap();
        assert_eq!(first_schema, second_schema);
        assert_eq!(
            first.target("value", &DataType::Int64).unwrap().name(),
            second.target("value", &DataType::Int64).unwrap().name()
        );
        assert_eq!(
            first.target("value", &DataType::Utf8).unwrap().name(),
            second.target("value", &DataType::Utf8).unwrap().name()
        );
    }

    #[test]
    fn timestamp_types_are_not_merged_by_suffix_equivalence() {
        let s = DataType::Timestamp(TimeUnit::Second, None);
        let ms = DataType::Timestamp(TimeUnit::Millisecond, None);
        let canonical = Schema::new(vec![field("time", s)]);
        let local = [Schema::new(vec![field("time", ms.clone())])];
        let (resolution, _) = resolve_local(canonical, &local, &[]).unwrap();
        assert_eq!(
            resolution.target("time", &ms).unwrap().name(),
            "time_timestamp_ms"
        );
    }

    #[tokio::test]
    async fn concurrent_absent_conflicts_retry_against_the_winner() {
        let backend = InMemoryBackend::new(None, Vec::new(), 2);
        let pinned = HashSet::new();
        let int_schema = Schema::new(vec![field("value", DataType::Int64)]);
        let utf8_schema = Schema::new(vec![field("value", DataType::Utf8)]);
        let int_schemas = [int_schema.clone()];
        let utf8_schemas = [utf8_schema.clone()];

        let (int_resolution, utf8_resolution) = tokio::join!(
            resolve_backend(&backend, "concurrent", &int_schemas, &pinned),
            resolve_backend(&backend, "concurrent", &utf8_schemas, &pinned),
        );
        let int_resolution = int_resolution.unwrap();
        let utf8_resolution = utf8_resolution.unwrap();
        let int_target = int_resolution.target("value", &DataType::Int64).unwrap();
        let utf8_target = utf8_resolution.target("value", &DataType::Utf8).unwrap();
        assert_ne!(int_target.name(), utf8_target.name());
        assert!(int_target.name() == "value" || utf8_target.name() == "value");

        let renamed_int = int_resolution.rename_schema(&int_schema).unwrap();
        let renamed_utf8 = utf8_resolution.rename_schema(&utf8_schema).unwrap();
        let canonical = backend.canonical_schema().unwrap();
        assert_schemas_merge_into(&canonical, [renamed_int, renamed_utf8]);
        assert_eq!(canonical.fields().len(), 2);
        assert!(backend.cas_attempt_count() >= 3);
        assert!(backend.global_read_count() >= 3);
    }

    #[tokio::test]
    async fn concurrent_existing_version_appends_independent_fields_without_loss() {
        let initial = Schema::new(vec![field("base", DataType::Boolean)]);
        let backend = InMemoryBackend::new(Some(initial), Vec::new(), 2);
        let pinned = HashSet::new();
        let left_schema = Schema::new(vec![field("left", DataType::Int64)]);
        let right_schema = Schema::new(vec![field("right", DataType::Utf8)]);
        let left_schemas = [left_schema.clone()];
        let right_schemas = [right_schema.clone()];

        let (left_resolution, right_resolution) = tokio::join!(
            resolve_backend(&backend, "append", &left_schemas, &pinned),
            resolve_backend(&backend, "append", &right_schemas, &pinned),
        );
        let left = left_resolution
            .unwrap()
            .rename_schema(&left_schema)
            .unwrap();
        let right = right_resolution
            .unwrap()
            .rename_schema(&right_schema)
            .unwrap();
        let canonical = backend.canonical_schema().unwrap();

        assert!(canonical.field_with_name("base").is_ok());
        assert!(canonical.field_with_name("left").is_ok());
        assert!(canonical.field_with_name("right").is_ok());
        assert_schemas_merge_into(&canonical, [left, right]);
        assert_eq!(backend.legacy_read_count(), 0);
        assert!(backend.cas_attempt_count() >= 3);
    }

    #[tokio::test]
    async fn legacy_schemas_are_loaded_only_when_global_schema_is_absent() {
        let legacy = Schema::new(vec![field("legacy", DataType::Utf8)]);
        let backend = InMemoryBackend::new(None, vec![legacy], 0);
        let local = [Schema::new(vec![field("new", DataType::Int64)])];
        let pinned = HashSet::new();

        resolve_backend(&backend, "bootstrap", &local, &pinned)
            .await
            .unwrap();
        assert_eq!(backend.legacy_read_count(), 1);
        let second = [Schema::new(vec![field("legacy", DataType::Utf8)])];
        resolve_backend(&backend, "bootstrap", &second, &pinned)
            .await
            .unwrap();
        assert_eq!(backend.legacy_read_count(), 1);
        assert!(
            backend
                .canonical_schema()
                .unwrap()
                .field_with_name("legacy")
                .is_ok()
        );
    }

    #[tokio::test]
    async fn repeated_cas_contention_returns_bounded_exhaustion_error() {
        let initial = Schema::new(vec![field("base", DataType::Boolean)]);
        let backend = InMemoryBackend::new(Some(initial), Vec::new(), 0).with_contention();
        let local = [Schema::new(vec![field("new", DataType::Int64)])];
        let error = resolve_backend(&backend, "contended", &local, &HashSet::new())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            MetastoreError::JsonSchemaError { message }
                if message.contains("did not converge")
        ));
        assert_eq!(backend.cas_attempt_count(), MAX_CAS_ATTEMPTS);
        assert_eq!(backend.global_read_count(), MAX_CAS_ATTEMPTS);
    }

    #[tokio::test]
    async fn backend_storage_errors_propagate_without_becoming_contention() {
        let local = [Schema::new(vec![field("new", DataType::Int64)])];
        let read_failure = InMemoryBackend::new(None, Vec::new(), 0).with_read_error();
        let read_error = resolve_backend(&read_failure, "read-error", &local, &HashSet::new())
            .await
            .unwrap_err();
        assert!(is_injected_storage_error(&read_error));
        assert_eq!(read_failure.cas_attempt_count(), 0);
        assert_eq!(read_failure.global_read_count(), 1);

        let write_failure = InMemoryBackend::new(None, Vec::new(), 0).with_write_error();
        let write_error = resolve_backend(&write_failure, "write-error", &local, &HashSet::new())
            .await
            .unwrap_err();
        assert!(is_injected_storage_error(&write_error));
        assert_eq!(write_failure.cas_attempt_count(), 1);
        assert_eq!(write_failure.global_read_count(), 1);
    }
}
