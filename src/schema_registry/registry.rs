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

//! Pure (I/O free) write-once field registry and the resolution rule.

use std::collections::{HashMap, HashSet};

use arrow_schema::{DataType, Field};

use crate::event::format::get_datatype_suffix;

use super::protocol::{FieldResolution, Outcome};

/// Max number of `_<suffix>` steps before a field is declared unresolvable.
pub const MAX_SUFFIX_DEPTH: usize = 3;

/// Types that are the same column type for registry purposes.
pub fn normalize(data_type: &DataType) -> DataType {
    match data_type {
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        other => other.clone(),
    }
}

pub fn compatible(registered: &DataType, proposed: &DataType) -> bool {
    normalize(registered) == normalize(proposed)
}

/// Authoritative name -> type map of one stream.
#[derive(Debug, Default, Clone)]
pub struct StreamRegistry {
    pub fields: HashMap<String, DataType>,
    pub version: u64,
    /// Columns that must never be renamed (time partition, custom partitions, p_* columns).
    pub reserved: HashSet<String>,
}

/// Result of resolving one request against a registry snapshot.
#[derive(Debug, Default)]
pub struct Resolution {
    pub resolutions: Vec<FieldResolution>,
    /// Names first registered by this request, in registration order.
    pub registered: Vec<(String, DataType)>,
}

impl StreamRegistry {
    /// Resolves `proposals` without mutating `self`. Deterministic: the answer is a pure
    /// function of the registry contents and the proposals, so retries are idempotent.
    pub fn resolve(&self, proposals: &[Field]) -> Resolution {
        let mut overlay: HashMap<String, DataType> = HashMap::new();
        let mut out = Resolution::default();

        for proposal in proposals {
            let proposed = proposal.data_type();
            if proposed.is_null() {
                continue;
            }
            let original = proposal.name().to_string();
            let lookup = |name: &str, overlay: &HashMap<String, DataType>| {
                overlay.get(name).or_else(|| self.fields.get(name)).cloned()
            };
            let registry_type = lookup(&original, &overlay);

            let mut name = original.clone();
            let mut depth = 0;
            let resolution = loop {
                match lookup(&name, &overlay) {
                    None => {
                        overlay.insert(name.clone(), proposed.clone());
                        out.registered.push((name.clone(), proposed.clone()));
                        break FieldResolution {
                            original_name: original.clone(),
                            proposed_type: proposed.clone(),
                            resolved_name: name.clone(),
                            data_type: proposed.clone(),
                            registry_type: Some(if depth == 0 {
                                proposed.clone()
                            } else {
                                registry_type.clone().expect("conflict implies registered")
                            }),
                            outcome: if depth == 0 {
                                Outcome::Accepted
                            } else {
                                Outcome::Renamed
                            },
                        };
                    }
                    Some(existing) if compatible(&existing, proposed) => {
                        break FieldResolution {
                            original_name: original.clone(),
                            proposed_type: proposed.clone(),
                            resolved_name: name.clone(),
                            data_type: existing,
                            registry_type: registry_type.clone(),
                            outcome: if depth == 0 {
                                Outcome::Accepted
                            } else {
                                Outcome::AlreadyRenamed
                            },
                        };
                    }
                    Some(_) => {
                        depth += 1;
                        if self.reserved.contains(&original) || depth > MAX_SUFFIX_DEPTH {
                            break FieldResolution {
                                original_name: original.clone(),
                                proposed_type: proposed.clone(),
                                resolved_name: original.clone(),
                                data_type: proposed.clone(),
                                registry_type: registry_type.clone(),
                                outcome: Outcome::Unresolvable,
                            };
                        }
                        name = format!("{name}_{}", get_datatype_suffix(proposed));
                    }
                }
            };
            out.resolutions.push(resolution);
        }
        out
    }

    pub fn apply(&mut self, registered: &[(String, DataType)]) {
        if registered.is_empty() {
            return;
        }
        for (name, data_type) in registered {
            self.fields
                .entry(name.clone())
                .or_insert_with(|| data_type.clone());
        }
        self.version += 1;
    }

    /// Seeds from an existing schema. The first type seen for a name wins; later conflicting
    /// types are returned so the caller can log them.
    pub fn seed<'a>(
        &mut self,
        fields: impl IntoIterator<Item = &'a Field>,
    ) -> Vec<(String, DataType, DataType)> {
        let mut conflicts = Vec::new();
        for field in fields {
            if field.data_type().is_null() {
                continue;
            }
            match self.fields.get(field.name()) {
                None => {
                    self.fields
                        .insert(field.name().clone(), field.data_type().clone());
                }
                Some(existing) if compatible(existing, field.data_type()) => {}
                Some(existing) => conflicts.push((
                    field.name().clone(),
                    existing.clone(),
                    field.data_type().clone(),
                )),
            }
        }
        conflicts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;

    fn f(name: &str, t: DataType) -> Field {
        Field::new(name, t, true)
    }

    fn reg(fields: &[(&str, DataType)]) -> StreamRegistry {
        StreamRegistry {
            fields: fields
                .iter()
                .map(|(n, t)| (n.to_string(), t.clone()))
                .collect(),
            version: 1,
            reserved: HashSet::new(),
        }
    }

    #[test]
    fn unknown_field_is_registered() {
        let r = reg(&[]);
        let res = r.resolve(&[f("a", DataType::Int64)]);
        assert_eq!(res.resolutions[0].outcome, Outcome::Accepted);
        assert_eq!(res.resolutions[0].resolved_name, "a");
        assert_eq!(res.registered, vec![("a".to_string(), DataType::Int64)]);
    }

    #[test]
    fn same_type_is_accepted_without_registration() {
        let r = reg(&[("a", DataType::Int64)]);
        let res = r.resolve(&[f("a", DataType::Int64)]);
        assert_eq!(res.resolutions[0].outcome, Outcome::Accepted);
        assert!(res.registered.is_empty());
    }

    #[test]
    fn conflicting_type_is_renamed_with_local_suffix_rule() {
        let r = reg(&[("a", DataType::Int64)]);
        let res = r.resolve(&[f("a", DataType::Utf8)]);
        let fr = &res.resolutions[0];
        assert_eq!(fr.outcome, Outcome::Renamed);
        assert_eq!(fr.resolved_name, "a_utf8");
        assert_eq!(fr.registry_type, Some(DataType::Int64));
        assert_eq!(res.registered, vec![("a_utf8".to_string(), DataType::Utf8)]);
    }

    #[test]
    fn existing_rename_target_is_reused() {
        let r = reg(&[("a", DataType::Int64), ("a_utf8", DataType::Utf8)]);
        let res = r.resolve(&[f("a", DataType::Utf8)]);
        assert_eq!(res.resolutions[0].outcome, Outcome::AlreadyRenamed);
        assert_eq!(res.resolutions[0].resolved_name, "a_utf8");
        assert!(res.registered.is_empty());
    }

    #[test]
    fn rename_target_taken_by_other_type_chains_suffix() {
        let r = reg(&[("a", DataType::Int64), ("a_utf8", DataType::Boolean)]);
        let res = r.resolve(&[f("a", DataType::Utf8)]);
        assert_eq!(res.resolutions[0].resolved_name, "a_utf8_utf8");
        assert_eq!(res.resolutions[0].outcome, Outcome::Renamed);
    }

    #[test]
    fn suffix_chain_is_bounded() {
        let r = reg(&[
            ("a", DataType::Int64),
            ("a_utf8", DataType::Int64),
            ("a_utf8_utf8", DataType::Int64),
            ("a_utf8_utf8_utf8", DataType::Int64),
        ]);
        let res = r.resolve(&[f("a", DataType::Utf8)]);
        assert_eq!(res.resolutions[0].outcome, Outcome::Unresolvable);
        assert!(res.registered.is_empty());
    }

    #[test]
    fn reserved_columns_are_never_renamed() {
        let mut r = reg(&[("ts", DataType::Timestamp(TimeUnit::Millisecond, None))]);
        r.reserved.insert("ts".into());
        let res = r.resolve(&[f("ts", DataType::Utf8)]);
        assert_eq!(res.resolutions[0].outcome, Outcome::Unresolvable);
    }

    #[test]
    fn null_proposals_are_ignored() {
        let r = reg(&[]);
        let res = r.resolve(&[f("a", DataType::Null)]);
        assert!(res.resolutions.is_empty());
        assert!(res.registered.is_empty());
    }

    #[test]
    fn large_utf8_is_compatible_with_utf8() {
        let r = reg(&[("a", DataType::Utf8)]);
        let res = r.resolve(&[f("a", DataType::LargeUtf8)]);
        assert_eq!(res.resolutions[0].outcome, Outcome::Accepted);
    }

    #[test]
    fn same_request_conflict_uses_overlay() {
        let r = reg(&[]);
        let res = r.resolve(&[f("a", DataType::Int64), f("a", DataType::Utf8)]);
        assert_eq!(res.resolutions[0].resolved_name, "a");
        assert_eq!(res.resolutions[1].resolved_name, "a_utf8");
        assert_eq!(res.registered.len(), 2);
    }

    #[test]
    fn retry_after_apply_is_idempotent() {
        // Ingestor 1 registers a:Int64, ingestor 2 gets a_utf8, ingestor 1 retries.
        let mut r = reg(&[]);
        let first = r.resolve(&[f("a", DataType::Int64)]);
        r.apply(&first.registered);
        let second = r.resolve(&[f("a", DataType::Utf8)]);
        r.apply(&second.registered);
        let retry = r.resolve(&[f("a", DataType::Int64)]);
        assert_eq!(retry.resolutions[0].resolved_name, "a");
        assert_eq!(retry.resolutions[0].outcome, Outcome::Accepted);
        let retry2 = r.resolve(&[f("a", DataType::Utf8)]);
        assert_eq!(retry2.resolutions[0].resolved_name, "a_utf8");
        assert_eq!(r.version, 3);
    }

    #[test]
    fn seed_keeps_first_type_and_reports_conflicts() {
        let mut r = StreamRegistry::default();
        let a = f("a", DataType::Int64);
        let b = f("a", DataType::Utf8);
        let conflicts = r.seed([&a, &b]);
        assert_eq!(r.fields["a"], DataType::Int64);
        assert_eq!(conflicts.len(), 1);
    }
}
