// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Execution functions

use std::collections::HashMap;
use super::cli_context::CliSessionContext;
use super::object_storage::get_object_store;

use datafusion::common::instant::Instant;
use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::config::ConfigFileType;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::{collect, execute_stream, ExecutionPlanProperties};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::dialect_from_str;


/// run and execute SQL statements and commands, against a context with the given print options
pub async fn exec_from_commands(
    ctx: &dyn CliSessionContext,
    commands: Vec<String>,
    base_command: bool,
) -> Result<()> {
    if !base_command {
        const TRIES: usize = 3;
        let mut query_num = 1;
        let mut total_elapsed_per_iteration = vec![0.0; TRIES];
        for sql in commands.clone() {
            for iteration in 1..=TRIES {
                let start = Instant::now();
                exec_and_print(ctx, sql.clone()).await?;
                let elapsed = start.elapsed().as_secs_f64();
                total_elapsed_per_iteration[iteration - 1] += elapsed;
                println!("Query {query_num} iteration {iteration} took {elapsed} seconds");
            }
            query_num += 1;
        }
        for (iteration, total_elapsed) in total_elapsed_per_iteration.iter().enumerate() {
            println!(
                "Total time for iteration {}: {} seconds",
                iteration + 1,
                total_elapsed
            );
        }
    }
    exec_and_print(ctx, commands[0].clone()).await?;

    Ok(())
}

pub(super) async fn exec_and_print(
    ctx: &dyn CliSessionContext,
    sql: String,
) -> Result<()> {
    let task_ctx = ctx.task_ctx();
    let dialect = &task_ctx.session_config().options().sql_parser.dialect;
    let dialect = dialect_from_str(dialect).ok_or_else(|| {
        plan_datafusion_err!(
            "Unsupported SQL dialect: {dialect}. Available dialects: \
                 Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                 MsSQL, ClickHouse, BigQuery, Ansi."
        )
    })?;

    let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
    for statement in statements {
        
        let plan = create_plan(ctx, statement).await?;

        let df = ctx.execute_logical_plan(plan).await?;
        let physical_plan = df.create_physical_plan().await?;

        if physical_plan.boundedness().is_unbounded() {
            if physical_plan.pipeline_behavior() == EmissionType::Final {
                return plan_err!(
                    "The given query can generate a valid result only once \
                    the source finishes, but the source is unbounded"
                );
            }
            // As the input stream comes, we can generate results.
            // However, memory safety is not guaranteed.
            let _ = execute_stream(physical_plan, task_ctx.clone())?;
        } else {
            // Bounded stream; collected results are printed after all input consumed.
            let _ = collect(physical_plan, task_ctx.clone()).await?;
        }
    }

    Ok(())
}


fn config_file_type_from_str(ext: &str) -> Option<ConfigFileType> {
    match ext.to_lowercase().as_str() {
        "csv" => Some(ConfigFileType::CSV),
        "json" => Some(ConfigFileType::JSON),
        "parquet" => Some(ConfigFileType::PARQUET),
        _ => None,
    }
}

async fn create_plan(
    ctx: &dyn CliSessionContext,
    statement: Statement,
) -> Result<LogicalPlan, DataFusionError> {
    let mut plan = ctx.session_state().statement_to_plan(statement).await?;

    // Note that cmd is a mutable reference so that create_external_table function can remove all
    // datafusion-cli specific options before passing through to datafusion. Otherwise, datafusion
    // will raise Configuration errors.
    if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &plan {
        // To support custom formats, treat error as None
        let format = config_file_type_from_str(&cmd.file_type);
        register_object_store_and_config_extensions(ctx, &cmd.location, &cmd.options, format)
            .await?;
    }

    if let LogicalPlan::Copy(copy_to) = &mut plan {
        let format = config_file_type_from_str(&copy_to.file_type.get_ext());

        register_object_store_and_config_extensions(
            ctx,
            &copy_to.output_url,
            &copy_to.options,
            format,
        )
        .await?;
    }
    Ok(plan)
}

/// Asynchronously registers an object store and its configuration extensions
/// to the session context.
///
/// This function dynamically registers a cloud object store based on the given
/// location and options. It first parses the location to determine the scheme
/// and constructs the URL accordingly. Depending on the scheme, it also registers
/// relevant options. The function then alters the default table options with the
/// given custom options. Finally, it retrieves and registers the object store
/// in the session context.
///
/// # Parameters
///
/// * `ctx`: A reference to the `SessionContext` for registering the object store.
/// * `location`: A string reference representing the location of the object store.
/// * `options`: A reference to a hash map containing configuration options for
///   the object store.
///
/// # Returns
///
/// A `Result<()>` which is an Ok value indicating successful registration, or
/// an error upon failure.
///
/// # Errors
///
/// This function can return an error if the location parsing fails, options
/// alteration fails, or if the object store cannot be retrieved and registered
/// successfully.
pub(crate) async fn register_object_store_and_config_extensions(
    ctx: &dyn CliSessionContext,
    location: &String,
    options: &HashMap<String, String>,
    format: Option<ConfigFileType>,
) -> Result<()> {
    // Parse the location URL to extract the scheme and other components
    let table_path = ListingTableUrl::parse(location)?;

    // Extract the scheme (e.g., "s3", "gcs") from the parsed URL
    let scheme = table_path.scheme();

    // Obtain a reference to the URL
    let url = table_path.as_ref();

    // Register the options based on the scheme extracted from the location
    ctx.register_table_options_extension_from_scheme(scheme);

    // Clone and modify the default table options based on the provided options
    let mut table_options = ctx.session_state().default_table_options();
    if let Some(format) = format {
        table_options.set_config_format(format);
    }
    table_options.alter_with_string_hash_map(options)?;

    // Retrieve the appropriate object store based on the scheme, URL, and modified table options
    let store = get_object_store(&ctx.session_state(), scheme, url, &table_options).await?;

    // Register the retrieved object store in the session context's runtime environment
    ctx.register_object_store(url, store);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::common::plan_err;

    use datafusion::prelude::SessionContext;
    use url::Url;

    async fn create_external_table_test(location: &str, sql: &str) -> Result<()> {
        let ctx = SessionContext::new();
        let plan = ctx.state().create_logical_plan(sql).await?;

        if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &plan {
            let format = config_file_type_from_str(&cmd.file_type);
            register_object_store_and_config_extensions(&ctx, &cmd.location, &cmd.options, format)
                .await?;
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        // Ensure the URL is supported by the object store
        ctx.runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        Ok(())
    }

    async fn copy_to_table_test(location: &str, sql: &str) -> Result<()> {
        let ctx = SessionContext::new();
        // AWS CONFIG register.

        let plan = ctx.state().create_logical_plan(sql).await?;

        if let LogicalPlan::Copy(cmd) = &plan {
            let format = config_file_type_from_str(&cmd.file_type.get_ext());
            register_object_store_and_config_extensions(
                &ctx,
                &cmd.output_url,
                &cmd.options,
                format,
            )
            .await?;
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        // Ensure the URL is supported by the object store
        ctx.runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_http() -> Result<()> {
        // Should be OK
        let location = "http://example.com/file.parquet";
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }
    #[tokio::test]
    async fn copy_to_external_object_store_test() -> Result<()> {
        let locations = vec![
            "s3://bucket/path/file.parquet",
            "oss://bucket/path/file.parquet",
            "cos://bucket/path/file.parquet",
            "gcs://bucket/path/file.parquet",
        ];
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let dialect = &task_ctx.session_config().options().sql_parser.dialect;
        let dialect = dialect_from_str(dialect).ok_or_else(|| {
            plan_datafusion_err!(
                "Unsupported SQL dialect: {dialect}. Available dialects: \
                 Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                 MsSQL, ClickHouse, BigQuery, Ansi."
            )
        })?;
        for location in locations {
            let sql = format!("copy (values (1,2)) to '{}' STORED AS PARQUET;", location);
            let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
            for statement in statements {
                //Should not fail
                let mut plan = create_plan(&ctx, statement).await?;
                if let LogicalPlan::Copy(copy_to) = &mut plan {
                    assert_eq!(copy_to.output_url, location);
                    assert_eq!(copy_to.file_type.get_ext(), "parquet".to_string());
                    ctx.runtime_env()
                        .object_store_registry
                        .get_store(&Url::parse(&copy_to.output_url).unwrap())?;
                } else {
                    return plan_err!("LogicalPlan is not a CopyTo");
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn copy_to_object_store_table_s3() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let location = "s3://bucket/path/file.parquet";

        // Missing region, use object_store defaults
        let sql = format!("COPY (values (1,2)) TO '{location}' STORED AS PARQUET
            OPTIONS ('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}')");
        copy_to_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_s3() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let region = "fake_us-east-2";
        let session_token = "fake_session_token";
        let location = "s3://bucket/path/file.parquet";

        // Missing region, use object_store defaults
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.region' '{region}', 'aws.session_token' '{session_token}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_oss() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "fake_endpoint";
        let location = "oss://bucket/path/file.parquet";

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.oss.endpoint' '{endpoint}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_cos() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "fake_endpoint";
        let location = "cos://bucket/path/file.parquet";

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.cos.endpoint' '{endpoint}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_gcs() -> Result<()> {
        let service_account_path = "fake_service_account_path";
        let service_account_key =
            "{\"private_key\": \"fake_private_key.pem\",\"client_email\":\"fake_client_email\", \"private_key_id\":\"id\"}";
        let application_credentials_path = "fake_application_credentials_path";
        let location = "gcs://bucket/path/file.parquet";

        // for service_account_path
        let sql = format!(
            "CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('gcp.service_account_path' '{service_account_path}') LOCATION '{location}'"
        );
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("os error 2"));

        // for service_account_key
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('gcp.service_account_key' '{service_account_key}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("No RSA key found in pem file"), "{err}");

        // for application_credentials_path
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('gcp.application_credentials_path' '{application_credentials_path}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("os error 2"));

        Ok(())
    }

    #[tokio::test]
    async fn create_external_table_local_file() -> Result<()> {
        let location = "path/to/file.parquet";

        // Ensure that local files are also registered
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET LOCATION '{location}'");
        create_external_table_test(location, &sql).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn create_external_table_format_option() -> Result<()> {
        let location = "path/to/file.cvs";

        // Test with format options
        let sql =
            format!("CREATE EXTERNAL TABLE test STORED AS CSV LOCATION '{location}' OPTIONS('format.has_header' 'true')");
        create_external_table_test(location, &sql).await.unwrap();

        Ok(())
    }
}
