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

use std::{collections::HashMap, env, fs, time::Instant};

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

pub async fn clickbench_benchmark() -> Result<impl Responder, actix_web::Error> {
    let results = tokio::task::spawn_blocking(run_benchmark)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?
        .map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(results)
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

    let parquet_file = env::var("PARQUET_FILE")?;
    register_hits(&ctx, &parquet_file).await?;
    let mut query_list = Vec::new();
    let queries_file = env::var("QUERIES_FILE")?;
    let queries = fs::read_to_string(queries_file)?;
    for query in queries.lines() {
        query_list.push(query.to_string());
    }
    execute_queries(&ctx, query_list).await
}

async fn register_hits(ctx: &SessionContext, parquet_file: &str) -> Result<(), anyhow::Error> {
    let options: ParquetReadOptions<'_> = Default::default();
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
    let mut results = Vec::new();

    for sql in query_list.iter() {
        let mut elapsed_times = Vec::new();
        for _iteration in 1..=TRIES {
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
            let statement = statements.front().unwrap();
            let plan = ctx.state().statement_to_plan(statement.clone()).await?;

            let df = ctx.execute_logical_plan(plan).await?;
            let physical_plan = df.create_physical_plan().await?;

            let _ = collect(physical_plan, task_ctx.clone()).await?;
            let elapsed = start.elapsed().as_secs_f64();
            elapsed_times.push(elapsed);
        }
        results.push(elapsed_times);
    }

    let result_json = json!(results);

    Ok(Json(result_json))
}
