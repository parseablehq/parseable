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

use std::{collections::HashMap, env, fs, process::Command, time::Instant};

use actix_web::{web::Json, Responder};
use datafusion::{
    common::plan_datafusion_err,
    error::DataFusionError,
    execution::{runtime_env::RuntimeEnvBuilder, SessionStateBuilder},
    physical_plan::collect,
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
    sql::{parser::DFParser, sqlparser::dialect::dialect_from_str},
};
use serde_json::{json, Value};
use tracing::warn;
static PARQUET_FILE: &str = "PARQUET_FILE";
static QUERIES_FILE: &str = "QUERIES_FILE";

pub async fn clickbench_benchmark() -> Result<impl Responder, actix_web::Error> {
    drop_system_caches()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    let results = tokio::task::spawn_blocking(run_benchmark)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?
        .map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(results)
}

pub async fn drop_system_caches() -> Result<(), anyhow::Error> {
    // Sync to flush file system buffers
    Command::new("sync")
        .status()
        .expect("Failed to execute sync command");
    let _ = Command::new("sudo")
        .args(["sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"])
        .output()
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;

    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
pub async fn run_benchmark() -> Result<Json<Value>, anyhow::Error> {
    let mut session_config = SessionConfig::from_env()?.with_information_schema(true);

    session_config = session_config.with_batch_size(8192);

    let rt_builder = RuntimeEnvBuilder::new();
    // set memory pool size
    let runtime_env = rt_builder.build_arc()?;
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .build();
    state
        .catalog_list()
        .catalog(&state.config_options().catalog.default_catalog)
        .expect("default catalog is provided by datafusion");

    let ctx = SessionContext::new_with_state(state);

    let mut table_options = HashMap::new();
    table_options.insert("binary_as_string", "true");

    let parquet_file = env::var(PARQUET_FILE)
        .map_err(|_| anyhow::anyhow!("PARQUET_FILE environment variable not set. Please set it to the path of the hits.parquet file."))?;
    register_hits(&ctx, &parquet_file).await?;
    println!("hits registered");
    let mut query_list = Vec::new();
    let queries_file = env::var(QUERIES_FILE)
     .map_err(|_| anyhow::anyhow!("QUERIES_FILE environment variable not set. Please set it to the path of the queries file."))?;
    let queries = fs::read_to_string(queries_file)?;
    for query in queries.lines() {
        query_list.push(query.to_string());
    }
    execute_queries(&ctx, query_list).await
}

async fn register_hits(ctx: &SessionContext, parquet_file: &str) -> Result<(), anyhow::Error> {
    let options: ParquetReadOptions<'_> = ParquetReadOptions::default();
    ctx.register_parquet("hits", parquet_file, options)
        .await
        .map_err(|e| {
            DataFusionError::Context(format!("Registering 'hits' as {parquet_file}"), Box::new(e))
        })?;
    Ok(())
}

pub async fn execute_queries(
    ctx: &SessionContext,
    query_list: Vec<String>,
) -> Result<Json<Value>, anyhow::Error> {
    const TRIES: usize = 3;
    let mut results = Vec::with_capacity(query_list.len());
    let mut query_count = 1;
    let mut total_elapsed_per_iteration = [0.0; TRIES];

    for (query_index, sql) in query_list.iter().enumerate() {
        let mut elapsed_times = Vec::with_capacity(TRIES);
        for iteration in 1..=TRIES {
            let start = Instant::now();
            let task_ctx = ctx.task_ctx();
            let dialect = &task_ctx.session_config().options().sql_parser.dialect;
            let dialect = dialect_from_str(dialect).ok_or_else(|| {
                plan_datafusion_err!(
                    "Unsupported SQL dialect: {dialect}. Available dialects: \
                      Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                      MsSQL, ClickHouse, BigQuery, Ansi."
                )
            })?;

            let statements = DFParser::parse_sql_with_dialect(sql, dialect.as_ref())?;
            let statement = statements
                .front()
                .ok_or_else(|| anyhow::anyhow!("No SQL statement found in query: {}", sql))?;
            let plan = ctx.state().statement_to_plan(statement.clone()).await?;

            let df = ctx.execute_logical_plan(plan).await?;
            let physical_plan = df.create_physical_plan().await?;

            let _ = collect(physical_plan, task_ctx.clone()).await?;
            let elapsed = start.elapsed().as_secs_f64();
            total_elapsed_per_iteration[iteration - 1] += elapsed;

            warn!("query {query_count} iteration {iteration} completed in {elapsed} secs");
            elapsed_times.push(elapsed);
        }
        query_count += 1;
        results.push(json!({
         "query_index": query_index,
         "query": sql,
         "elapsed_times": elapsed_times
        }));
    }
    for (iteration, total_elapsed) in total_elapsed_per_iteration.iter().enumerate() {
        warn!(
            "Total time for iteration {}: {} seconds",
            iteration + 1,
            total_elapsed
        );
    }

    let result_json = json!(results);

    Ok(Json(result_json))
}
