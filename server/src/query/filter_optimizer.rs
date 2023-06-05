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

use std::{collections::HashMap, sync::Arc};

use datafusion::{
    common::{DFField, DFSchema},
    logical_expr::{Filter, LogicalPlan, Projection},
    optimizer::{optimize_children, OptimizerRule},
    prelude::{lit, or, Column, Expr},
};

/// Rewrites logical plan for source using projection and filter  
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
        // if there are no patterns then the rule cannot be performed
        let Some(filter_expr) = self.expr() else { return Ok(None); } ;
        // if filter ( tags ) - table_scan pattern encountered then do not apply
        if let LogicalPlan::Filter(Filter { predicate, .. }) = plan {
            if predicate == &filter_expr {
                return Ok(None);
            }
        }

        match plan {
            // cannot apply when no projection is set on the table
            LogicalPlan::TableScan(table) if table.projection.is_none() => return Ok(None),
            LogicalPlan::TableScan(table) => {
                let mut table = table.clone();
                let schema = &table.source.schema();
                let tags_index = schema.index_of(&self.column)?;
                let tags_field = schema.field(tags_index);

                // modify source table projection to include tags
                let mut df_schema = table.projected_schema.fields().clone();
                df_schema.push(DFField::new(
                    Some(table.table_name.clone()),
                    tags_field.name(),
                    tags_field.data_type().clone(),
                    tags_field.is_nullable(),
                ));

                table.projected_schema =
                    Arc::new(DFSchema::new_with_metadata(df_schema, HashMap::default())?);

                if let Some(projection) = &mut table.projection {
                    projection.push(tags_index)
                }

                let projected_schema = table.projected_schema.clone();
                let filter = LogicalPlan::Filter(Filter::try_new(
                    filter_expr,
                    Arc::new(LogicalPlan::TableScan(table)),
                )?);
                let plan = LogicalPlan::Projection(Projection::new_from_schema(
                    Arc::new(filter),
                    projected_schema,
                ));

                return Ok(Some(plan));
            }
            _ => (),
        }

        // If we didn't find anything then recurse as normal and build the result.
        optimize_children(self, plan, config)
    }

    fn name(&self) -> &str {
        "parseable_read_filter"
    }
}

impl FilterOptimizerRule {
    fn expr(&self) -> Option<Expr> {
        let mut patterns = self.literals.iter().map(|literal| {
            Expr::Column(Column::from_name(&self.column)).like(lit(format!("%{}%", literal)))
        });

        let Some(mut filter_expr) = patterns.next() else { return None };
        for expr in patterns {
            filter_expr = or(filter_expr, expr)
        }

        Some(filter_expr)
    }
}
