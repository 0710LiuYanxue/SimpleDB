use crate::logical_plan::expression::AggregateFunc;
use crate::logical_plan::schema::NaiveSchema;
use crate::physical_plan::CrossJoin;
use crate::physical_plan::HashJoin;

use crate::physical_plan::avg::Avg;
use crate::physical_plan::count::Count;
use crate::physical_plan::max::Max;
use crate::physical_plan::min::Min;
use crate::physical_plan::sum::Sum;
use crate::physical_plan::PhysicalAggregatePlan;
use crate::physical_plan::PhysicalBinaryExpr;
use crate::physical_plan::PhysicalExprRef;
use crate::physical_plan::PhysicalLimitPlan;
use crate::physical_plan::PhysicalLiteralExpr;
use crate::physical_plan::PhysicalOffsetPlan;
use crate::physical_plan::PhysicalPlanRef;
use crate::physical_plan::SelectionPlan;
use crate::physical_plan::UpdatePlan;   // lyx 新增一个UpdatePlan
use crate::physical_plan::InsertPlan;   // lyx 新增一个InsertPlan
use crate::physical_plan::DeletePlan;   // lyx 新增一个DeletePlan
use crate::physical_plan::CreateTablePlan;   // lyx 新增一个CreateTablePlan
use crate::{
    error::{ErrorCode, Result},
    logical_plan::{
        expression::{Column, LogicalExpr},
        plan::LogicalPlan,
    },
    physical_plan::{ColumnExpr, ProjectionPlan, ScanPlan},
};

// 查询规划器（QueryPlanner）通过递归的方式，将不同类型的逻辑计划（LogicalPlan）
// 转换为对应的物理计划（PhysicalPlan），即为每个逻辑操作（例如 TableScan、Projection、Join 等）生成相应的物理执行计划。
pub struct QueryPlanner;

impl QueryPlanner {
    // 核心方法，根据传入的逻辑计划生成物理计划。
    // 它通过模式匹配（match）对不同类型的逻辑计划进行处理，返回相应的物理计划。
    pub fn create_physical_plan(plan: &LogicalPlan) -> Result<PhysicalPlanRef> {
        match plan {
            // 调用 ScanPlan::create 方法，生成一个物理表扫描计划。
            // ScanPlan 需要提供表的源和可选的列投影。
            LogicalPlan::TableScan(table_scan) => Ok(ScanPlan::create(
                table_scan.source.clone(),
                table_scan.projection.clone(),
            )),
            LogicalPlan::CreateTable(create_table) => {
                Ok(CreateTablePlan::create(create_table.schema.clone()))
            }
            LogicalPlan::Delete(delete) => {
                let input = Self::create_physical_plan(&delete.input)?;
                let conditions = Self::create_physical_expression(&delete.conditions, plan)?;
                Ok(DeletePlan::create(input, conditions, delete.source.clone()))
            }
            LogicalPlan::Insert(insert) => {
                let input = Self::create_physical_plan(&insert.input)?;
                Ok(InsertPlan::create( insert.source.clone(), input))
            }
            LogicalPlan::Update(update) => {
                let input = Self::create_physical_plan(&update.input)?;
                let conditions = Self::create_physical_expression(&update.conditions, plan)?;
                Ok(UpdatePlan::create(input, conditions,update.assignments.clone()))
            }
            // Projection 表示一个列选择操作（即 SELECT 子句中的列）。
            // 输入包括输入计划、列的表达式、和输出的字段模式
            LogicalPlan::Projection(proj) => {
                let input = Self::create_physical_plan(&proj.input)?;
                let proj_expr = proj
                    .exprs
                    .iter()
                    .map(|expr| Self::create_physical_expression(expr, &proj.input).unwrap())
                    .collect::<Vec<_>>();
                let fields = proj
                    .exprs
                    .iter()
                    .map(|expr| expr.data_field(proj.input.as_ref()).unwrap())
                    .collect::<Vec<_>>();
                let proj_schema = NaiveSchema::new(fields);
                Ok(ProjectionPlan::create(input, proj_schema, proj_expr))
            }
            LogicalPlan::Limit(limit) => {
                let plan = Self::create_physical_plan(&limit.input)?;
                Ok(PhysicalLimitPlan::create(plan, limit.n))
            }
            LogicalPlan::Offset(offset) => {
                let plan = Self::create_physical_plan(&offset.input)?;
                Ok(PhysicalOffsetPlan::create(plan, offset.n))
            }
            // 对于连接操作，代码生成 HashJoin 物理计划。HashJoin 是一种高效的连接算法，它使用哈希表来实现连接。
            LogicalPlan::Join(join) => {
                let left = Self::create_physical_plan(&join.left)?;
                let right = Self::create_physical_plan(&join.right)?;
                // 这里目前是使用的哈希连接算法，后续可以考虑改用其他算法。
                Ok(HashJoin::create(
                    left,
                    right,
                    join.on.clone(),
                    join.join_type,
                    join.schema.clone(),
                ))
            }
            LogicalPlan::Filter(filter) => {
                let predicate = Self::create_physical_expression(&filter.predicate, plan)?;
                let input = Self::create_physical_plan(&filter.input)?;
                Ok(SelectionPlan::create(input, predicate))
            }
            // 聚合操作，处理聚合函数Count、Sum、Avg、Max、Min。
            LogicalPlan::Aggregate(aggr) => {
                let mut group_exprs = vec![];
                for group_expr in &aggr.group_expr {
                    group_exprs.push(Self::create_physical_expression(group_expr, &aggr.input)?);
                }

                let mut aggr_ops = vec![];
                for aggr_expr in &aggr.aggr_expr {
                    let aggr_op = match aggr_expr.fun {
                        AggregateFunc::Count => {
                            let expr =
                                Self::create_physical_expression(&aggr_expr.args, &aggr.input)?;
                            let col_expr = expr.as_any().downcast_ref::<ColumnExpr>();
                            if let Some(col_expr) = col_expr {
                                Count::create(col_expr.clone())
                            } else {
                                return Err(ErrorCode::PlanError(
                                    "Aggregate Func should have a column in it".to_string(),
                                ));
                            }
                        }
                        AggregateFunc::Sum => {
                            let expr =
                                Self::create_physical_expression(&aggr_expr.args, &aggr.input)?;
                            let col_expr = expr.as_any().downcast_ref::<ColumnExpr>();
                            if let Some(col_expr) = col_expr {
                                Sum::create(col_expr.clone())
                            } else {
                                return Err(ErrorCode::PlanError(
                                    "Aggregate Func should have a column in it".to_string(),
                                ));
                            }
                        }
                        AggregateFunc::Avg => {
                            let expr =
                                Self::create_physical_expression(&aggr_expr.args, &aggr.input)?;
                            let col_expr = expr.as_any().downcast_ref::<ColumnExpr>();
                            if let Some(col_expr) = col_expr {
                                Avg::create(col_expr.clone())
                            } else {
                                return Err(ErrorCode::PlanError(
                                    "Aggregate Func should have a column in it".to_string(),
                                ));
                            }
                        }
                        AggregateFunc::Min => {
                            let expr =
                                Self::create_physical_expression(&aggr_expr.args, &aggr.input)?;
                            let col_expr = expr.as_any().downcast_ref::<ColumnExpr>();
                            if let Some(col_expr) = col_expr {
                                Min::create(col_expr.clone())
                            } else {
                                return Err(ErrorCode::PlanError(
                                    "Aggregate Func should have a column in it".to_string(),
                                ));
                            }
                        }
                        AggregateFunc::Max => {
                            let expr =
                                Self::create_physical_expression(&aggr_expr.args, &aggr.input)?;
                            let col_expr = expr.as_any().downcast_ref::<ColumnExpr>();
                            if let Some(col_expr) = col_expr {
                                Max::create(col_expr.clone())
                            } else {
                                return Err(ErrorCode::PlanError(
                                    "Aggregate Func should have a column in it".to_string(),
                                ));
                            }
                        }
                    };
                    aggr_ops.push(aggr_op);
                }

                let input = Self::create_physical_plan(&aggr.input)?;
                Ok(PhysicalAggregatePlan::create(group_exprs, aggr_ops, input))
            }
            // 对于交叉连接，即没有指定连接条件的连接，我们直接使用笛卡尔积的方式进行连接
            LogicalPlan::CrossJoin(join) => {
                let left = Self::create_physical_plan(&join.left)?;
                let right = Self::create_physical_plan(&join.right)?;
                Ok(CrossJoin::create(
                    left,
                    right,
                    join.join_type,
                    join.schema.clone(),
                ))
            }
        }
    }

    // 将查询中存在的逻辑表达式LogicalExpr转换为物理表达式PhysicalExpr
    pub fn create_physical_expression(
        expr: &LogicalExpr,
        input: &LogicalPlan,
    ) -> Result<PhysicalExprRef> {
        match expr {
            LogicalExpr::Alias(_, _) => todo!(),
            // 对于列引用，我们需要找到对应的列索引，并生成 ColumnExpr。 这是最简单的情况，也是我们目前所需的。
            LogicalExpr::Column(Column { name, .. }) => {
                for (idx, field) in input.schema().fields().iter().enumerate() {
                    if field.name() == name {
                        return ColumnExpr::try_create(None, Some(idx));
                    }
                }
                Err(ErrorCode::ColumnNotExists(format!(
                    "column `{}` not exists",
                    name
                )))
            }
            // 对于常量表达式，我们生成一个 PhysicalLiteralExpr。
            LogicalExpr::Literal(scalar_val) => Ok(PhysicalLiteralExpr::create(scalar_val.clone())),
            // 对于二元表达式，我们递归地生成左右子表达式，并生成 PhysicalBinaryExpr。
            LogicalExpr::BinaryExpr(bin_expr) => {
                let left = Self::create_physical_expression(bin_expr.left.as_ref(), input)?;
                let right = Self::create_physical_expression(bin_expr.right.as_ref(), input)?;
                let phy_bin_expr = PhysicalBinaryExpr::create(left, bin_expr.op.clone(), right);
                Ok(phy_bin_expr)
            }
            LogicalExpr::AggregateFunction(_) => todo!(),
            LogicalExpr::Wildcard => todo!(),
        }
    }
}

