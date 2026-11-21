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

//! Ingestor-side cache of what the authority decided for a stream.

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use super::{
    protocol::{FieldResolution, Outcome, RegistryEntry},
    registry::compatible,
};

pub const VIEW_FILE_NAME: &str = ".field_resolution.json";

/// Resolved fields and renames for one stream on one ingestor.
///
/// Append-only by construction (it mirrors a write-once registry), so an older snapshot is
/// always a prefix-preserving subset of a newer one: a file converts the same way under any
/// later view.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ResolvedView {
    pub registry_version: u64,
    /// Authoritative type per column name, including rename targets.
    pub fields: HashMap<String, DataType>,
    /// (name, type written in the arrow file) -> registered columns it may be written to, in
    /// the order the authority granted them. Append-only: a file whose earlier target is
    /// taken by another of its columns uses the next one, and existing entries never change.
    pub renames: HashMap<(String, DataType), Vec<String>>,
    /// (name, type) pairs the authority could not place. Files carrying them are not converted.
    pub unresolvable: HashSet<(String, DataType)>,
}

#[derive(Serialize, Deserialize)]
struct RenameEntry {
    from: String,
    from_type: DataType,
    to: String,
}

#[derive(Serialize, Deserialize, Default)]
struct ViewFile {
    registry_version: u64,
    fields: Vec<RegistryEntry>,
    renames: Vec<RenameEntry>,
}

/// Outcome of checking one `(name, type)` against the view.
#[derive(Debug, PartialEq, Eq)]
pub enum FieldState {
    Resolved,
    Unresolved,
    Unresolvable,
}

impl ResolvedView {
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty() && self.renames.is_empty()
    }

    pub fn field_state(&self, name: &str, data_type: &DataType) -> FieldState {
        if data_type.is_null() {
            return FieldState::Resolved;
        }
        if self
            .fields
            .get(name)
            .is_some_and(|t| compatible(t, data_type))
            || self
                .renames
                .contains_key(&(name.to_string(), data_type.clone()))
        {
            FieldState::Resolved
        } else if self
            .unresolvable
            .contains(&(name.to_string(), data_type.clone()))
        {
            FieldState::Unresolvable
        } else {
            FieldState::Unresolved
        }
    }

    /// Whether a batch field typed `incoming` predates a type the authority set for `name`:
    /// the stream's type `current` is the authority's, and `incoming` is another one.
    ///
    /// Such a batch was built against the schema from before a resolution changed the
    /// type (a stream switches to the authority's type whatever told it so: a rename, a
    /// registry snapshot or a restart). It must still be accepted: its arrow file carries
    /// `incoming`, which conversion resolves like any other unresolved type, renaming it or
    /// holding the file back.
    pub fn predates_resolved_type(
        &self,
        name: &str,
        current: &DataType,
        incoming: &DataType,
    ) -> bool {
        current != incoming
            && self
                .fields
                .get(name)
                .is_some_and(|resolved| compatible(resolved, current))
    }

    /// Folds an authority answer into a new view.
    pub fn with_resolutions(
        &self,
        version: u64,
        resolutions: &[FieldResolution],
        delta: &[RegistryEntry],
    ) -> Self {
        let mut next = self.clone();
        for entry in delta {
            next.fields
                .insert(entry.name.clone(), entry.data_type.clone());
        }
        for r in resolutions {
            let key = (r.original_name.clone(), r.proposed_type.clone());
            match r.outcome {
                Outcome::Unresolvable => {
                    next.unresolvable.insert(key);
                    if let Some(t) = &r.registry_type {
                        next.fields.insert(r.original_name.clone(), t.clone());
                    }
                }
                Outcome::Accepted | Outcome::Renamed | Outcome::AlreadyRenamed => {
                    next.unresolvable.remove(&key);
                    next.fields
                        .insert(r.resolved_name.clone(), r.data_type.clone());
                    if let Some(t) = &r.registry_type {
                        next.fields.insert(r.original_name.clone(), t.clone());
                    }
                    if r.resolved_name != r.original_name {
                        let targets = next.renames.entry(key).or_default();
                        if !targets.contains(&r.resolved_name) {
                            targets.push(r.resolved_name.clone());
                        }
                    }
                }
            }
        }
        next.registry_version = next.registry_version.max(version);
        next
    }

    /// Returns the renamed schema of an arrow file, or `None` when no column is renamed.
    ///
    /// Each renamed column takes its first target not used by another column of the file.
    /// Fails when every target is taken; the file then needs a new target from the authority.
    pub fn rename_schema(&self, schema: &Schema) -> Result<Option<Arc<Schema>>, ArrowError> {
        if self.renames.is_empty() {
            return Ok(None);
        }
        let targets = |f: &Field| {
            self.renames
                .get(&(f.name().clone(), f.data_type().clone()))
                .filter(|t| !t.is_empty())
        };
        if !schema.fields().iter().any(|f| targets(f).is_some()) {
            return Ok(None);
        }
        let mut taken: HashSet<&str> = schema
            .fields()
            .iter()
            .filter(|f| targets(f).is_none())
            .map(|f| f.name().as_str())
            .collect();
        let mut fields = Vec::with_capacity(schema.fields().len());
        for f in schema.fields() {
            let Some(candidates) = targets(f) else {
                fields.push(f.clone());
                continue;
            };
            let Some(to) = candidates.iter().find(|t| !taken.contains(t.as_str())) else {
                return Err(ArrowError::SchemaError(format!(
                    "renaming {} produced a duplicate column: {} already taken",
                    f.name(),
                    candidates.join(", ")
                )));
            };
            taken.insert(to);
            fields.push(Arc::new(f.as_ref().clone().with_name(to)));
        }
        Ok(Some(Arc::new(Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        ))))
    }

    pub fn load(dir: &Path) -> Self {
        let path = dir.join(VIEW_FILE_NAME);
        let Ok(bytes) = std::fs::read(&path) else {
            return Self::default();
        };
        match serde_json::from_slice::<ViewFile>(&bytes) {
            Ok(file) => Self {
                registry_version: file.registry_version,
                fields: file
                    .fields
                    .into_iter()
                    .map(|e| (e.name, e.data_type))
                    .collect(),
                renames: file.renames.into_iter().fold(
                    HashMap::<_, Vec<String>>::new(),
                    |mut acc, r| {
                        acc.entry((r.from, r.from_type)).or_default().push(r.to);
                        acc
                    },
                ),
                unresolvable: HashSet::new(),
            },
            Err(err) => {
                tracing::warn!("ignoring unreadable {}: {err}", path.display());
                Self::default()
            }
        }
    }

    /// Atomically persists the view (tmp file + rename).
    pub fn persist(&self, dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let mut fields: Vec<RegistryEntry> = self
            .fields
            .iter()
            .map(|(name, data_type)| RegistryEntry {
                name: name.clone(),
                data_type: data_type.clone(),
            })
            .collect();
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        // One entry per target, in grant order: `load` rebuilds each list in file order.
        let mut keys: Vec<&(String, DataType)> = self.renames.keys().collect();
        keys.sort_by_cached_key(|(from, from_type)| (from.clone(), format!("{from_type:?}")));
        let renames: Vec<RenameEntry> = keys
            .into_iter()
            .flat_map(|key| {
                self.renames[key].iter().map(|to| RenameEntry {
                    from: key.0.clone(),
                    from_type: key.1.clone(),
                    to: to.clone(),
                })
            })
            .collect();
        let file = ViewFile {
            registry_version: self.registry_version,
            fields,
            renames,
        };
        let bytes = serde_json::to_vec(&file).map_err(std::io::Error::other)?;
        let tmp = dir.join(format!("{VIEW_FILE_NAME}.tmp"));
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, dir.join(VIEW_FILE_NAME))
    }
}

/// Zero-copy rename of a batch to `schema` (same column order, new names).
pub fn rename_batch(
    schema: &Option<Arc<Schema>>,
    batch: RecordBatch,
) -> Result<RecordBatch, ArrowError> {
    match schema {
        Some(schema) => RecordBatch::try_new(schema.clone(), batch.columns().to_vec()),
        None => Ok(batch),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, StringArray};

    fn renamed_view() -> ResolvedView {
        ResolvedView::default().with_resolutions(
            2,
            &[FieldResolution {
                original_name: "a".into(),
                proposed_type: DataType::Utf8,
                resolved_name: "a_utf8".into(),
                data_type: DataType::Utf8,
                registry_type: Some(DataType::Int64),
                outcome: Outcome::Renamed,
            }],
            &[],
        )
    }

    #[test]
    fn renamed_field_is_resolved_and_winner_type_recorded() {
        let v = renamed_view();
        assert_eq!(v.field_state("a", &DataType::Utf8), FieldState::Resolved);
        assert_eq!(v.field_state("a", &DataType::Int64), FieldState::Resolved);
        assert_eq!(v.field_state("b", &DataType::Int64), FieldState::Unresolved);
        assert_eq!(v.fields["a"], DataType::Int64);
        assert_eq!(v.fields["a_utf8"], DataType::Utf8);
        assert_eq!(v.registry_version, 2);
    }

    #[test]
    fn rename_is_zero_copy() {
        let v = renamed_view();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let renamed = v.rename_schema(&schema).unwrap().expect("renamed");
        assert_eq!(renamed.field(0).name(), "a_utf8");
        assert_eq!(renamed.field(1).name(), "b");

        let a: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![a.clone(), b.clone()]).unwrap();
        let out = rename_batch(&Some(renamed), batch).unwrap();
        assert!(Arc::ptr_eq(out.column(0), &a));
        assert_eq!(out.schema().field(0).name(), "a_utf8");
    }

    #[test]
    fn winner_typed_column_is_not_renamed() {
        let v = renamed_view();
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        assert!(v.rename_schema(&schema).unwrap().is_none());
    }

    #[test]
    fn duplicate_after_rename_is_an_error() {
        let v = renamed_view();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("a_utf8", DataType::Utf8, true),
        ]);
        assert!(v.rename_schema(&schema).is_err());
    }

    /// `a: Utf8` granted `a_utf8`, then `a_utf8_utf8` for a file that also carries `a_utf8`.
    fn two_target_view() -> ResolvedView {
        renamed_view().with_resolutions(
            3,
            &[FieldResolution {
                original_name: "a".into(),
                proposed_type: DataType::Utf8,
                resolved_name: "a_utf8_utf8".into(),
                data_type: DataType::Utf8,
                registry_type: Some(DataType::Int64),
                outcome: Outcome::Renamed,
            }],
            &[],
        )
    }

    fn renamed_names(v: &ResolvedView, fields: &[&str]) -> Vec<String> {
        let schema = Schema::new(
            fields
                .iter()
                .map(|n| Field::new(*n, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        );
        v.rename_schema(&schema)
            .unwrap()
            .expect("renamed")
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    #[test]
    fn later_targets_never_change_earlier_renames() {
        let v = two_target_view();
        assert_eq!(
            v.renames[&("a".to_string(), DataType::Utf8)],
            vec!["a_utf8".to_string(), "a_utf8_utf8".to_string()]
        );
        // Files that converted under the first answer still convert the same way.
        assert_eq!(renamed_names(&v, &["a"]), vec!["a_utf8"]);
        assert_eq!(
            renamed_names(&v, &["a", "a_utf8_utf8"]),
            vec!["a_utf8", "a_utf8_utf8"]
        );
        // Only a file whose first target is taken moves to the next one.
        assert_eq!(
            renamed_names(&v, &["a", "a_utf8"]),
            vec!["a_utf8_utf8", "a_utf8"]
        );
        // Every target taken: needs a new one from the authority.
        let all = Schema::new(
            ["a", "a_utf8", "a_utf8_utf8"]
                .iter()
                .map(|n| Field::new(*n, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        );
        assert!(v.rename_schema(&all).is_err());
    }

    #[test]
    fn persist_and_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let v = renamed_view();
        v.persist(dir.path()).unwrap();
        assert_eq!(ResolvedView::load(dir.path()), v);
        let v = two_target_view();
        v.persist(dir.path()).unwrap();
        assert_eq!(ResolvedView::load(dir.path()), v);
    }

    /// A type learned only from a registry snapshot carries no rename, yet the stream
    /// switches to it; a batch still carrying the local type must be accepted.
    #[test]
    fn snapshot_type_tolerates_batches_typed_before_it() {
        let v = ResolvedView::default().with_resolutions(
            4,
            &[],
            &[RegistryEntry {
                name: "b".into(),
                data_type: DataType::Float64,
            }],
        );
        assert!(v.renames.is_empty());
        // The stream switched to the authority's type: an older Utf8 batch predates it.
        assert!(v.predates_resolved_type("b", &DataType::Float64, &DataType::Utf8));
        // Same type: nothing to tolerate.
        assert!(!v.predates_resolved_type("b", &DataType::Float64, &DataType::Float64));
        // The stream's type is not the authority's: a real conflict, not a stale batch.
        assert!(!v.predates_resolved_type("b", &DataType::Utf8, &DataType::Float64));
        // Nothing resolved for the name.
        assert!(!v.predates_resolved_type("c", &DataType::Float64, &DataType::Utf8));
        // A renamed type is covered the same way.
        let v = renamed_view();
        assert!(v.predates_resolved_type("a", &DataType::Int64, &DataType::Utf8));
    }

    #[test]
    fn missing_or_corrupt_file_is_empty_view() {
        let dir = tempfile::tempdir().unwrap();
        assert!(ResolvedView::load(dir.path()).is_empty());
        std::fs::write(dir.path().join(VIEW_FILE_NAME), b"{not json").unwrap();
        assert!(ResolvedView::load(dir.path()).is_empty());
    }
}
