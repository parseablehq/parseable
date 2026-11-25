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

//! Cluster-wide schema resolution for distributed ingestion.
//!
//! Ingestors infer types independently. Before converting arrow files to parquet they ask
//! a single authority (the querier, or Prism) which type each new field has. The authority
//! keeps a write-once name registry per stream; a later conflicting type is written under
//! `<name>_<typesuffix>`, the same rule the ingest-time conflict path uses.
//! See `docs/schema-resolution/` for the design.

pub mod authority;
pub mod client;
pub mod ingestor;
pub mod protocol;
pub mod registry;
pub mod view;

use std::{collections::HashMap, sync::Arc};

use arrow_schema::{Field, Schema};
use relative_path::RelativePathBuf;
use tracing::warn;

use crate::storage::{SCHEMA_FILE_NAME, STREAM_ROOT_DIRECTORY};

/// Node metadata of every node of `node_type`, tenant-scoped first, then cluster-wide.
pub async fn nodes_of_type(
    node_type: crate::handlers::http::modal::NodeType,
    tenant_id: &Option<String>,
) -> anyhow::Result<Vec<crate::handlers::http::modal::NodeMetadata>> {
    use crate::handlers::http::cluster::get_node_info;
    let mut nodes = get_node_info(node_type.clone(), tenant_id).await?;
    if nodes.is_empty() && tenant_id.is_some() {
        nodes = get_node_info(node_type, &None).await?;
    }
    Ok(nodes)
}

/// Which node type is the schema authority: Prism when the cluster has one registered
/// (enterprise), otherwise the querier. Decided from registered metadata, not liveness, so
/// a Prism that is briefly down never causes ingestors to fall back to a second authority.
pub async fn authority_node_type(
    tenant_id: &Option<String>,
) -> anyhow::Result<crate::handlers::http::modal::NodeType> {
    use crate::handlers::http::modal::NodeType;
    if nodes_of_type(NodeType::Prism, tenant_id).await?.is_empty() {
        Ok(NodeType::Querier)
    } else {
        Ok(NodeType::Prism)
    }
}

/// Path of the stream-level (authority owned) schema, independent of this node's mode.
pub fn authoritative_schema_path(stream: &str, tenant_id: &Option<String>) -> RelativePathBuf {
    let tenant = tenant_id.as_deref().unwrap_or("");
    RelativePathBuf::from_iter([tenant, stream, STREAM_ROOT_DIRECTORY, SCHEMA_FILE_NAME])
}

/// Strictly merges schemas read from storage, ignoring the registry version.
///
/// The authority stamps its registry version into the stream-level schema's metadata, and a
/// node that builds its schema from all stored schemas copies the stamp into its own file.
/// Once the authority moves on, the copies disagree, and a strict merge would refuse them
/// (a node then cannot start). Only the highest version is kept: the authority's.
pub fn merge_stored_schemas(
    schemas: impl IntoIterator<Item = Schema>,
) -> Result<Schema, arrow_schema::ArrowError> {
    let mut version: Option<u64> = None;
    let schemas: Vec<Schema> = schemas
        .into_iter()
        .map(|schema| {
            let mut metadata = schema.metadata().clone();
            if let Some(v) = metadata
                .remove(authority::REGISTRY_VERSION_KEY)
                .and_then(|v| v.parse::<u64>().ok())
            {
                version = Some(version.map_or(v, |cur| cur.max(v)));
            }
            Schema::new_with_metadata(schema.fields().clone(), metadata)
        })
        .collect();
    let mut merged = Schema::try_merge(schemas)?;
    if let Some(version) = version {
        merged.metadata.insert(
            authority::REGISTRY_VERSION_KEY.to_owned(),
            version.to_string(),
        );
    }
    Ok(merged)
}

/// Unions schemas without ever failing: the first type seen for a field name wins and
/// later conflicting types are skipped with a warning. Metadata of the first schema is kept.
pub fn merge_schemas_tolerant(stream: &str, schemas: impl IntoIterator<Item = Schema>) -> Schema {
    let mut fields: Vec<Field> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut metadata = None;

    for schema in schemas {
        if metadata.is_none() {
            metadata = Some(schema.metadata().clone());
        }
        for field in schema.fields() {
            match index.get(field.name()) {
                None => {
                    index.insert(field.name().clone(), fields.len());
                    fields.push(field.as_ref().clone());
                }
                Some(&i) => {
                    let mut merged = fields[i].clone();
                    if merged.try_merge(field).is_ok() {
                        fields[i] = merged;
                    } else {
                        warn!(
                            stream,
                            "schema conflict on field {}: keeping {}, skipping {}",
                            field.name(),
                            fields[i].data_type(),
                            field.data_type()
                        );
                    }
                }
            }
        }
    }

    Schema::new_with_metadata(
        fields.into_iter().map(Arc::new).collect::<Vec<_>>(),
        metadata.unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    #[test]
    fn stored_schemas_with_different_registry_versions_merge() {
        // The authority's `.schema` at version 18, a node's file with a stale copy (17) and a
        // node's file without one: a node restarting now must still build its schema.
        let with_version = |fields: Vec<Field>, v: Option<&str>| {
            let metadata = v
                .map(|v| {
                    HashMap::from([(authority::REGISTRY_VERSION_KEY.to_owned(), v.to_owned())])
                })
                .unwrap_or_default();
            Schema::new_with_metadata(fields, metadata)
        };
        let merged = merge_stored_schemas([
            with_version(vec![Field::new("a", DataType::Utf8, true)], Some("18")),
            with_version(vec![Field::new("b", DataType::Int64, true)], Some("17")),
            with_version(vec![Field::new("a", DataType::Utf8, true)], None),
        ])
        .unwrap();
        assert_eq!(merged.fields().len(), 2);
        assert_eq!(merged.metadata()[authority::REGISTRY_VERSION_KEY], "18");

        // Without any stamp nothing is added, and a real type conflict still fails.
        assert!(
            merge_stored_schemas([with_version(
                vec![Field::new("a", DataType::Utf8, true)],
                None
            )])
            .unwrap()
            .metadata()
            .is_empty()
        );
        assert!(
            merge_stored_schemas([
                with_version(vec![Field::new("a", DataType::Utf8, true)], Some("1")),
                with_version(vec![Field::new("a", DataType::Int64, true)], Some("2")),
            ])
            .is_err()
        );
    }

    #[test]
    fn tolerant_merge_keeps_first_type() {
        let a = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let b = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("c", DataType::Boolean, true),
        ]);
        let merged = merge_schemas_tolerant("s", [a, b]);
        assert_eq!(merged.fields().len(), 3);
        assert_eq!(
            merged.field_with_name("a").unwrap().data_type(),
            &DataType::Int64
        );
        assert!(merged.field_with_name("c").is_ok());
    }

    #[test]
    fn authoritative_path_ignores_node_mode() {
        assert_eq!(
            authoritative_schema_path("s", &None).as_str(),
            "s/.stream/.schema"
        );
        assert_eq!(
            authoritative_schema_path("s", &Some("t".into())).as_str(),
            "t/s/.stream/.schema"
        );
    }
}
