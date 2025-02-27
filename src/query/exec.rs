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

// use super::cli_context::CliSessionContext;
// use super::object_storage::get_object_store;
use datafusion::common::instant::Instant;
use datafusion::common::plan_datafusion_err;
use datafusion::error::Result;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::dialect_from_str;

/// run and execute SQL statements and commands, against a context with the given print options
pub async fn exec_from_commands(
    ctx: &SessionContext,
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

pub(super) async fn exec_and_print(ctx: &SessionContext, sql: String) -> Result<()> {
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
    let statement = statements.front().unwrap();
    let plan = ctx.state().statement_to_plan(statement.clone()).await?;

    let df = ctx.execute_logical_plan(plan).await?;
    let physical_plan = df.create_physical_plan().await?;

    // Bounded stream; collected results are printed after all input consumed.
    let results = collect(physical_plan, task_ctx.clone()).await?;
    println!("{:?}", results);
    Ok(())
}