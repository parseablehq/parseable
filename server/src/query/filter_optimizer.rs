/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use std::sync::Arc;

use datafusion::{
    logical_expr::{Filter, LogicalPlan},
    optimizer::{optimize_children, OptimizerRule},
    prelude::{lit, or, Column, Expr},
};

pub struct FilterOptimizerRule {
    pub column: String,
    pub literals: Vec<String>,
}

impl OptimizerRule for FilterOptimizerRule {
    fn try_optimize(
        &self,
        plan: &datafusion::logical_expr::LogicalPlan,
        config: &dyn datafusion::optimizer::OptimizerConfig,
    ) -> datafusion::error::Result<Option<datafusion::logical_expr::LogicalPlan>> {
        let mut patterns = self.literals.iter().map(|literal| {
            Expr::Column(Column::from_name(&self.column)).like(lit(format!("%{}%", literal)))
        });

        let Some(mut filter_expr) = patterns.next() else { return Ok(None) };
        for expr in patterns {
            filter_expr = or(filter_expr, expr)
        }

        // Do not apply optimization when node is already there
        // maybe this is second pass
        if let LogicalPlan::Filter(Filter {
            predicate: expr, ..
        }) = plan
        {
            if expr == &filter_expr {
                return Ok(None);
            }
        }

        if let table_scan @ LogicalPlan::TableScan(_) = plan {
            let filter = Filter::try_new(filter_expr, Arc::new(table_scan.clone())).unwrap();
            return Ok(Some(LogicalPlan::Filter(filter)));
        }

        // If we didn't find anything then recurse as normal and build the result.
        optimize_children(self, plan, config)
    }

    fn name(&self) -> &str {
        "parseable_read_filter"
    }
}
