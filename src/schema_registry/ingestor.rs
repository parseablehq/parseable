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

//! Per-stream resolution state on an ingestor.
//!
//! What is "unresolved" is derived from the schemas of the arrow files about to be
//! converted, so the state survives restarts without extra bookkeeping. The only
//! persisted piece is the [`ResolvedView`] (authority answers), kept in the stream's
//! staging directory.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::File,
    hash::{Hash, Hasher},
    io::BufReader,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
};

use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};
use tracing::{error, warn};
use ulid::Ulid;

use super::{
    client::ResolveFailure,
    protocol::{PROTOCOL_VERSION, ResolveRequest, ResolveResponse},
    view::{FieldState, ResolvedView},
};

pub type Resolver<'a> = dyn Fn(ResolveRequest) -> Result<ResolveResponse, ResolveFailure> + 'a;

/// Everything `ensure_resolved` needs to know about the stream and the node.
pub struct ResolveContext<'a> {
    pub stream: &'a str,
    pub tenant_id: &'a Option<String>,
    pub node_id: String,
    pub data_path: &'a Path,
    pub max_deferred_cycles: u32,
    pub resolver: &'a Resolver<'a>,
}

pub struct ResolutionOutcome {
    /// Files whose every `(name, type)` is resolved; safe to convert.
    pub ready: Vec<PathBuf>,
    /// Files that must stay unconverted for now.
    pub deferred: Vec<PathBuf>,
}

#[derive(Default)]
pub struct ResolutionState {
    blocked: AtomicBool,
    deferred_cycles: AtomicU32,
    /// Serialises "classify -> RPC -> apply" for one stream.
    resolve_lock: Mutex<()>,
    view: RwLock<Arc<ResolvedView>>,
    /// Request id reused for retries of the same unresolved field set.
    pending_request: Mutex<Option<(u64, Ulid)>>,
    last_error: Mutex<Option<String>>,
}

impl ResolutionState {
    pub fn load(data_path: &Path) -> Self {
        Self {
            view: RwLock::new(Arc::new(ResolvedView::load(data_path))),
            ..Default::default()
        }
    }

    pub fn view(&self) -> Arc<ResolvedView> {
        self.view.read().expect("view lock poisoned").clone()
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked.load(Ordering::Acquire)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().expect("lock poisoned").clone()
    }

    pub fn deferred_cycles(&self) -> u32 {
        self.deferred_cycles.load(Ordering::Acquire)
    }

    fn reset(&self) {
        self.deferred_cycles.store(0, Ordering::Release);
        self.blocked.store(false, Ordering::Release);
        *self.last_error.lock().expect("lock poisoned") = None;
    }

    fn record_failure(&self, ctx: &ResolveContext<'_>, reason: String) {
        let cycles = self.deferred_cycles.fetch_add(1, Ordering::AcqRel) + 1;
        *self.last_error.lock().expect("lock poisoned") = Some(reason.clone());
        if ctx.max_deferred_cycles > 0 && cycles >= ctx.max_deferred_cycles {
            if !self.blocked.swap(true, Ordering::AcqRel) {
                error!(
                    stream = ctx.stream,
                    "schema resolution failed for {cycles} cycles, rejecting ingestion until resolved: {reason}"
                );
            }
        } else {
            warn!(
                stream = ctx.stream,
                "schema resolution deferred (cycle {cycles}): {reason}"
            );
        }
    }

    /// Splits `files` into convertible and deferred, asking the authority about any
    /// field not yet resolved. `on_apply` runs after a new view was persisted and before
    /// it becomes visible, so the stream can align its in-memory and staged schemas.
    pub fn ensure_resolved(
        &self,
        ctx: &ResolveContext<'_>,
        files: Vec<PathBuf>,
        on_apply: impl FnOnce(&ResolvedView),
    ) -> ResolutionOutcome {
        let _guard = self.resolve_lock.lock().expect("resolve lock poisoned");

        let schemas: Vec<(PathBuf, Option<Arc<Schema>>)> = files
            .into_iter()
            .map(|path| {
                let schema = read_file_schema(&path);
                (path, schema)
            })
            .collect();

        let view = self.view();
        let unresolved = unresolved_fields(&view, &schemas);
        if unresolved.is_empty() {
            return self.finish(ctx, &view, schemas);
        }

        let fields: Vec<Field> = unresolved
            .iter()
            .map(|(name, data_type)| Field::new(name, data_type.clone(), true))
            .collect();
        let request_id = self.request_id(&fields);
        let request = ResolveRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            node_id: ctx.node_id.clone(),
            tenant_id: ctx.tenant_id.clone(),
            stream: ctx.stream.to_owned(),
            known_version: view.registry_version,
            fields,
        };

        match (ctx.resolver)(request) {
            Ok(response) => {
                let next =
                    view.with_resolutions(response.version, &response.resolutions, &response.delta);
                if let Err(err) = next.persist(ctx.data_path) {
                    // Without a durable view a restart could convert with stale renames;
                    // the answer is idempotent, so just ask again next cycle.
                    self.record_failure(ctx, format!("could not persist resolved fields: {err}"));
                    return self.split(&view, schemas);
                }
                on_apply(&next);
                let next = Arc::new(next);
                self.view
                    .write()
                    .expect("view lock poisoned")
                    .clone_from(&next);
                *self.pending_request.lock().expect("lock poisoned") = None;
                self.finish(ctx, &next, schemas)
            }
            Err(ResolveFailure::StreamDeleting) => {
                // The stream is going away; nothing to resolve and nothing to block.
                self.split(&view, schemas)
            }
            Err(err) => {
                self.record_failure(ctx, err.to_string());
                self.split(&view, schemas)
            }
        }
    }

    fn finish(
        &self,
        ctx: &ResolveContext<'_>,
        view: &ResolvedView,
        schemas: Vec<(PathBuf, Option<Arc<Schema>>)>,
    ) -> ResolutionOutcome {
        let mut blocking: BTreeSet<String> = schemas
            .iter()
            .filter_map(|(_, s)| s.as_ref())
            .flat_map(|s| s.fields().iter())
            .filter(|f| view.field_state(f.name(), f.data_type()) == FieldState::Unresolvable)
            .map(|f| format!("{} ({})", f.name(), f.data_type()))
            .collect();
        blocking.extend(
            schemas
                .iter()
                .filter_map(|(_, s)| s.as_ref())
                .filter_map(|s| view.rename_schema(s).err())
                .map(|e| e.to_string()),
        );
        let blocking: Vec<String> = blocking.into_iter().collect();
        let outcome = self.split(view, schemas);
        if blocking.is_empty() {
            self.reset();
        } else {
            self.record_failure(
                ctx,
                format!(
                    "fields cannot be reconciled with the cluster schema automatically: {}",
                    blocking.join(", ")
                ),
            );
        }
        outcome
    }

    fn split(
        &self,
        view: &ResolvedView,
        schemas: Vec<(PathBuf, Option<Arc<Schema>>)>,
    ) -> ResolutionOutcome {
        let mut ready = Vec::new();
        let mut deferred = Vec::new();
        for (path, schema) in schemas {
            let convertible =
                match &schema {
                    // Unreadable files are left to the conversion path, which already
                    // classifies and removes invalid arrow files.
                    None => true,
                    Some(schema) => {
                        schema.fields().iter().all(|f| {
                            view.field_state(f.name(), f.data_type()) == FieldState::Resolved
                        }) && view.rename_schema(schema).is_ok()
                    }
                };
            if convertible {
                ready.push(path);
            } else {
                deferred.push(path);
            }
        }
        ResolutionOutcome { ready, deferred }
    }

    fn request_id(&self, fields: &[Field]) -> Ulid {
        let mut hasher = std::collections::hash_map::DefaultHasher::default();
        for f in fields {
            f.name().hash(&mut hasher);
            f.data_type().hash(&mut hasher);
        }
        let key = hasher.finish();
        let mut pending = self.pending_request.lock().expect("lock poisoned");
        match *pending {
            Some((k, id)) if k == key => id,
            _ => {
                let id = Ulid::new();
                *pending = Some((key, id));
                id
            }
        }
    }
}

/// Reads only the schema message of an arrow IPC stream file.
fn read_file_schema(path: &Path) -> Option<Arc<Schema>> {
    let file = File::open(path).ok()?;
    StreamReader::try_new(BufReader::new(file), None)
        .ok()
        .map(|r| r.schema())
}

/// Distinct unresolved `(name, type)` pairs, sorted for a deterministic request.
fn unresolved_fields(
    view: &ResolvedView,
    schemas: &[(PathBuf, Option<Arc<Schema>>)],
) -> Vec<(String, DataType)> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for schema in schemas.iter().filter_map(|(_, s)| s.as_ref()) {
        // A cached rename can land on a column this file already carries (`a` -> `a_utf8`
        // next to a literal `a_utf8`). Send every column of such a file, so the authority
        // sees the names in use and picks a free target.
        let collides = view.rename_schema(schema).is_err();
        for f in schema.fields() {
            if f.data_type().is_null() {
                continue;
            }
            if (collides || view.field_state(f.name(), f.data_type()) == FieldState::Unresolved)
                && seen.insert((f.name().clone(), f.data_type().clone()))
            {
                out.push((f.name().clone(), f.data_type().clone()));
            }
        }
    }
    out.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| format!("{:?}", a.1).cmp(&format!("{:?}", b.1)))
    });
    out
}

/// Types the in-memory schema must switch to after a resolution: for every field whose
/// in-memory type differs from the authority's, the authority wins.
pub fn schema_overrides(
    view: &ResolvedView,
    current: &HashMap<String, Arc<Field>>,
) -> Vec<Arc<Field>> {
    let mut out = Vec::new();
    for (name, data_type) in &view.fields {
        match current.get(name) {
            Some(field) if field.data_type() != data_type => out.push(Arc::new(
                field.as_ref().clone().with_data_type(data_type.clone()),
            )),
            None if view.renames.values().any(|to| to == name) => {
                out.push(Arc::new(Field::new(name, data_type.clone(), true)))
            }
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, sync::Arc};

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_ipc::writer::StreamWriter;

    use super::*;
    use crate::schema_registry::{
        protocol::{FieldResolution, Outcome},
        registry::StreamRegistry,
    };

    fn write_arrow(dir: &Path, name: &str, field: Field, array: ArrayRef) -> PathBuf {
        let path = dir.join(name);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let mut w = StreamWriter::try_new(File::create(&path).unwrap(), &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
        path
    }

    fn ctx<'a>(
        dir: &'a Path,
        resolver: &'a Resolver<'a>,
        tenant: &'a Option<String>,
    ) -> ResolveContext<'a> {
        ResolveContext {
            stream: "s",
            tenant_id: tenant,
            node_id: "n".into(),
            data_path: dir,
            max_deferred_cycles: 3,
            resolver,
        }
    }

    /// A resolver backed by an in-process registry, like the querier.
    fn registry_resolver(
        registry: &RefCell<StreamRegistry>,
    ) -> impl Fn(ResolveRequest) -> Result<ResolveResponse, ResolveFailure> + '_ {
        move |req| {
            let mut reg = registry.borrow_mut();
            let res = reg.resolve(&req.fields);
            reg.apply(&res.registered);
            Ok(ResolveResponse {
                protocol_version: PROTOCOL_VERSION,
                request_id: req.request_id,
                version: reg.version,
                resolutions: res.resolutions,
                delta: Vec::new(),
            })
        }
    }

    #[test]
    fn two_ingestors_converge_on_one_type() {
        let registry = RefCell::new(StreamRegistry::default());
        let resolver = registry_resolver(&registry);
        let tenant = None;

        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let f1 = write_arrow(
            d1.path(),
            "a.arrows",
            Field::new("a", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1])),
        );
        let f2 = write_arrow(
            d2.path(),
            "a.arrows",
            Field::new("a", DataType::Utf8, true),
            Arc::new(StringArray::from(vec!["x"])),
        );

        let s1 = ResolutionState::default();
        let s2 = ResolutionState::default();
        let o1 = s1.ensure_resolved(
            &ctx(d1.path(), &resolver, &tenant),
            vec![f1.clone()],
            |_| {},
        );
        let o2 = s2.ensure_resolved(
            &ctx(d2.path(), &resolver, &tenant),
            vec![f2.clone()],
            |_| {},
        );
        assert_eq!(o1.ready, vec![f1]);
        assert_eq!(o2.ready, vec![f2.clone()]);

        let v2 = s2.view();
        let renamed = v2
            .rename_schema(&read_file_schema(&f2).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(renamed.field(0).name(), "a_utf8");
        assert_eq!(v2.fields["a"], DataType::Int64);
        // Persisted: a fresh state on the same dir needs no RPC.
        assert_eq!(
            ResolutionState::load(d2.path()).view().as_ref(),
            v2.as_ref()
        );
    }

    #[test]
    fn cached_rename_colliding_with_a_column_is_re_resolved() {
        let registry = RefCell::new(StreamRegistry::default());
        let resolver = registry_resolver(&registry);
        let tenant = None;
        let dir = tempfile::tempdir().unwrap();
        let state = ResolutionState::default();

        // Another node owns `a: Int64`; this node's first file maps `a: Utf8` to `a_utf8`.
        registry
            .borrow_mut()
            .apply(&[("a".to_string(), DataType::Int64)]);
        let first = write_arrow(
            dir.path(),
            "1.arrows",
            Field::new("a", DataType::Utf8, true),
            Arc::new(StringArray::from(vec!["x"])),
        );
        let out = state.ensure_resolved(&ctx(dir.path(), &resolver, &tenant), vec![first], |_| {});
        assert_eq!(out.deferred.len(), 0);
        assert_eq!(
            state.view().renames[&("a".to_string(), DataType::Utf8)],
            "a_utf8"
        );

        // A later file carries both `a: Utf8` and a literal `a_utf8`: both count as resolved,
        // but the cached rename would duplicate `a_utf8`.
        let path = dir.path().join("2.arrows");
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("a_utf8", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["y"])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut w = StreamWriter::try_new(File::create(&path).unwrap(), &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();

        let out = state.ensure_resolved(
            &ctx(dir.path(), &resolver, &tenant),
            vec![path.clone()],
            |_| {},
        );
        assert_eq!(out.ready, vec![path]);
        assert!(!state.is_blocked());
        let renamed = state.view().rename_schema(&schema).unwrap().unwrap();
        let names: Vec<_> = renamed.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["a_utf8_utf8", "a_utf8"]);
    }

    #[test]
    fn failures_defer_then_block_then_recover() {
        let dir = tempfile::tempdir().unwrap();
        let tenant = None;
        let file = write_arrow(
            dir.path(),
            "a.arrows",
            Field::new("a", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1])),
        );
        let failing = |_req: ResolveRequest| Err(ResolveFailure::Transient("down".into()));
        let state = ResolutionState::default();
        for cycle in 1..=3 {
            let out = state.ensure_resolved(
                &ctx(dir.path(), &failing, &tenant),
                vec![file.clone()],
                |_| {},
            );
            assert!(out.ready.is_empty());
            assert_eq!(out.deferred, vec![file.clone()]);
            assert_eq!(state.is_blocked(), cycle >= 3);
        }
        let registry = RefCell::new(StreamRegistry::default());
        let ok = registry_resolver(&registry);
        let out = state.ensure_resolved(&ctx(dir.path(), &ok, &tenant), vec![file.clone()], |_| {});
        assert_eq!(out.ready, vec![file]);
        assert!(!state.is_blocked());
        assert_eq!(state.deferred_cycles(), 0);
    }

    #[test]
    fn retries_reuse_request_id() {
        let dir = tempfile::tempdir().unwrap();
        let tenant = None;
        let file = write_arrow(
            dir.path(),
            "a.arrows",
            Field::new("a", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1])),
        );
        let ids = RefCell::new(Vec::new());
        let failing = |req: ResolveRequest| {
            ids.borrow_mut().push(req.request_id);
            Err(ResolveFailure::Transient("down".into()))
        };
        let state = ResolutionState::default();
        state.ensure_resolved(
            &ctx(dir.path(), &failing, &tenant),
            vec![file.clone()],
            |_| {},
        );
        state.ensure_resolved(&ctx(dir.path(), &failing, &tenant), vec![file], |_| {});
        let ids = ids.borrow();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], ids[1]);
    }

    #[test]
    fn unresolvable_field_blocks_its_files() {
        let dir = tempfile::tempdir().unwrap();
        let tenant = None;
        let file = write_arrow(
            dir.path(),
            "a.arrows",
            Field::new("ts", DataType::Utf8, true),
            Arc::new(StringArray::from(vec!["x"])),
        );
        let resolver = |req: ResolveRequest| {
            Ok(ResolveResponse {
                protocol_version: PROTOCOL_VERSION,
                request_id: req.request_id,
                version: 1,
                resolutions: vec![FieldResolution {
                    original_name: "ts".into(),
                    proposed_type: DataType::Utf8,
                    resolved_name: "ts".into(),
                    data_type: DataType::Utf8,
                    registry_type: None,
                    outcome: Outcome::Unresolvable,
                }],
                delta: Vec::new(),
            })
        };
        let state = ResolutionState::default();
        let out = state.ensure_resolved(
            &ctx(dir.path(), &resolver, &tenant),
            vec![file.clone()],
            |_| {},
        );
        assert_eq!(out.deferred, vec![file]);
        assert_eq!(state.deferred_cycles(), 1);
    }

    #[test]
    fn authority_without_resolution_defers() {
        let dir = tempfile::tempdir().unwrap();
        let tenant = None;
        let file = write_arrow(
            dir.path(),
            "a.arrows",
            Field::new("a", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1])),
        );
        let old = |_req: ResolveRequest| Err(ResolveFailure::Rejected("upgrade".into()));
        let state = ResolutionState::default();
        let out =
            state.ensure_resolved(&ctx(dir.path(), &old, &tenant), vec![file.clone()], |_| {});
        assert_eq!(out.deferred, vec![file]);
        assert_eq!(state.deferred_cycles(), 1);
    }

    #[test]
    fn overrides_flip_loser_type_and_add_rename_target() {
        let view = ResolvedView::default().with_resolutions(
            1,
            &[FieldResolution {
                original_name: "a".into(),
                proposed_type: DataType::Utf8,
                resolved_name: "a_utf8".into(),
                data_type: DataType::Utf8,
                registry_type: Some(DataType::Int64),
                outcome: Outcome::Renamed,
            }],
            &[],
        );
        let current: HashMap<String, Arc<Field>> = HashMap::from([(
            "a".to_string(),
            Arc::new(Field::new("a", DataType::Utf8, true)),
        )]);
        let mut o = schema_overrides(&view, &current);
        o.sort_by(|a, b| a.name().cmp(b.name()));
        assert_eq!(o.len(), 2);
        assert_eq!(o[0].data_type(), &DataType::Int64);
        assert_eq!(o[1].name(), "a_utf8");
    }
}
