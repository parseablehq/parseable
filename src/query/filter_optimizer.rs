// /*
//  * Parseable Server (C) 2022 - 2025 Parseable, Inc.
//  *
//  * This program is free software: you can redistribute it and/or modify
//  * it under the terms of the GNU Affero General Public License as
//  * published by the Free Software Foundation, either version 3 of the
//  * License, or (at your option) any later version.
//  *
//  * This program is distributed in the hope that it will be useful,
//  * but WITHOUT ANY WARRANTY; without even the implied warranty of
//  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  * GNU Affero General Public License for more details.
//  *
//  * You should have received a copy of the GNU Affero General Public License
//  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
//  *
//  */
// use std::{collections::HashMap, sync::Arc};

// use arrow_schema::Field;
// use datafusion::{
//     common::DFSchema,
//     logical_expr::{Filter, LogicalPlan, Projection},
//     optimizer::{optimize_children, OptimizerRule},
//     prelude::{lit, or, Column, Expr},
//     scalar::ScalarValue,
// };

// /// Rewrites logical plan for source using projection and filter
// pub struct FilterOptimizerRule {
//     pub column: String,
//     pub literals: Vec<String>,
// }

// // Try to add filter node on table scan
// // As every table supports projection push down
// // we try to directly add projection for column directly to table
// // To preserve the orignal projection we must add a projection node with orignal projection
// impl OptimizerRule for FilterOptimizerRule {
//     fn try_optimize(
//         &self,
//         plan: &datafusion::logical_expr::LogicalPlan,
//         config: &dyn datafusion::optimizer::OptimizerConfig,
//     ) -> datafusion::error::Result<Option<datafusion::logical_expr::LogicalPlan>> {
//         // if there are no patterns then the rule cannot be performed
//         let Some(filter_expr) = self.expr() else {
//             return Ok(None);
//         };

//         if let LogicalPlan::Filter(filter) = plan {
//             if filter.predicate == filter_expr {
//                 return Ok(None);
//             }
//         }

//         if let LogicalPlan::TableScan(table) = plan {
//             if table.projection.is_none()
//                 || table
//                     .filters
//                     .iter()
//                     .any(|expr| self.contains_valid_tag_filter(expr))
//             {
//                 return Ok(None);
//             }

//             let mut table = table.clone();
//             let schema = &table.source.schema();
//             let orignal_projection = table.projected_schema.clone();

//             // add filtered column projection to table
//             if !table
//                 .projected_schema
//                 .has_column_with_unqualified_name(&self.column)
//             {
//                 let tags_index = schema.index_of(&self.column)?;
//                 let tags_field = schema.field(tags_index);
//                 // modify source table projection to include tags
//                 let df_schema = table.projected_schema.fields().clone();

//                 // from datafusion 37.1.0 -> 40.0.0
//                 // `DFField` has been removed
//                 // `DFSchema.new_with_metadata()` has changed
//                 // it requires `qualified_fields`(`Vec<(Option<TableReference>, Arc<Field>)>`) instead of `fields`
//                 // hence, use `DFSchema::from_unqualified_fields()` for relatively unchanged code

//                 df_schema.to_vec().push(Arc::new(Field::new(
//                     tags_field.name(),
//                     tags_field.data_type().clone(),
//                     tags_field.is_nullable(),
//                 )));

//                 table.projected_schema =
//                     Arc::new(DFSchema::from_unqualified_fields(df_schema, HashMap::default())?);
//                 if let Some(projection) = &mut table.projection {
//                     projection.push(tags_index)
//                 }
//             }

//             let filter = LogicalPlan::Filter(Filter::try_new(
//                 filter_expr,
//                 Arc::new(LogicalPlan::TableScan(table)),
//             )?);
//             let plan = LogicalPlan::Projection(Projection::new_from_schema(
//                 Arc::new(filter),
//                 orignal_projection,
//             ));

//             return Ok(Some(plan));
//         }

//         // If we didn't find anything then recurse as normal and build the result.

//         // TODO: replace `optimize_children()` since it will be removed
//         // But it is not being used anywhere, so might as well just let it be for now
//         optimize_children(self, plan, config)
//     }

//     fn name(&self) -> &str {
//         "parseable_read_filter"
//     }
// }

// impl FilterOptimizerRule {
//     fn expr(&self) -> Option<Expr> {
//         let mut patterns = self.literals.iter().map(|literal| {
//             Expr::Column(Column::from_name(&self.column)).like(lit(format!("%{}%", literal)))
//         });

//         let mut filter_expr = patterns.next()?;
//         for expr in patterns {
//             filter_expr = or(filter_expr, expr)
//         }

//         Some(filter_expr)
//     }

//     fn contains_valid_tag_filter(&self, expr: &Expr) -> bool {
//         match expr {
//             Expr::Like(like) => {
//                 let matches_column = match &*like.expr {
//                     Expr::Column(column) => column.name == self.column,
//                     _ => return false,
//                 };

//                 let matches_pattern = match &*like.pattern {
//                     Expr::Literal(ScalarValue::Utf8(Some(literal))) => {
//                         let literal = literal.trim_matches('%');
//                         self.literals.iter().any(|x| x == literal)
//                     }
//                     _ => false,
//                 };

//                 matches_column && matches_pattern && !like.negated
//             }
//             _ => false,
//         }
//     }
// }
