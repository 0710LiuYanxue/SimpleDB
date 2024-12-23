use std::sync::Arc;

use crate::logical_plan::expression::LogicalExpr;
use crate::logical_plan::plan::{Aggregate, Filter, LogicalPlan, Projection, Update, Delete, CreateTable};   // lyx 增加了一个update
use sqlparser::ast::{Assignment, Ident, SetExpr}; 
use super::expression::{AggregateFunction, Column};
use super::plan::{Insert, Join, JoinType, Limit, Offset};
use super::schema::NaiveSchema;
use crate::error::{ErrorCode, Result};
use crate::datasource::TableRef;

#[derive(Clone)]
pub struct DataFrame {
    pub plan: LogicalPlan,
}

impl DataFrame {
    // DataFrame 的构造函数。它接受一个 LogicalPlan 作为参数，并返回一个新的 DataFrame 实例。
    pub fn new(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    // project 方法用于进行 投影 操作，即选择某些列（字段）。
    // 它接受一个包含 LogicalExpr 的 Vec，每个 LogicalExpr 表示一个字段或计算。
    // 对于每个表达式，使用 data_field 方法获取字段信息，最终生成一个新的 NaiveSchema，表示查询后的数据模式（即表的结构）。
    // 然后，返回一个新的 DataFrame，其中的 plan 被更新为 LogicalPlan::Projection，表示执行投影操作。
    pub fn project(self, exprs: Vec<LogicalExpr>) -> Result<Self> {
        // TODO(veeupup): Ambiguous reference of field
        let mut fields = vec![];
        for expr in &exprs {
            fields.push(expr.data_field(&self.plan)?);
        }
        let schema = NaiveSchema::new(fields);
        Ok(Self {
            plan: LogicalPlan::Projection(Projection {
                input: Arc::new(self.plan),
                exprs,
                schema,
            }),
        })
    }

    // filter 方法用于进行 过滤 操作，即对数据进行条件筛选。它接受一个 LogicalExpr 表达式，表示过滤条件。
    // 该方法返回一个新的 DataFrame，其中的 plan 被更新为 LogicalPlan::Filter，表示执行过滤操作。
    pub fn filter(self, expr: LogicalExpr) -> Self {
        Self {
            plan: LogicalPlan::Filter(Filter {
                input: Arc::new(self.plan),
                predicate: expr,
            }),
        }
    }

    pub fn create_table(self, table_name: String, schema: NaiveSchema) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::CreateTable(CreateTable {
                table_name,
                schema,
            }),
        })
    }

    // update方法执行 更新操作 的一个dataframe
    pub fn update(self, conditions: LogicalExpr, assignments: Vec<Assignment> ) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::Update(Update {
                input: Arc::new(self.plan),
                conditions,
                assignments,
            }),
        })
    }
    // insert方法执行 插入操作 的一个dataframe
    pub fn insert(self, columns: Vec<Ident>,source: SetExpr ) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::Insert(Insert {
                input: Arc::new(self.plan),
                columns,
                source,
            }),
        })
    }   

    pub fn delete(self, source: TableRef, conditions: LogicalExpr) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::Delete(Delete {
                input: Arc::new(self.plan),
                source,
                conditions,
            }),
        })
    }
    // aggregate 方法用于执行 聚合 操作。它接受两个参数：
    // group_expr：表示分组的字段。
    // aggr_expr：表示聚合函数（如 SUM, COUNT 等）。
    // 首先，方法将 group_expr 和 aggr_expr 转换为字段，并将它们合并成一个字段集合。
    // 然后，生成一个新的 NaiveSchema，表示聚合后的数据模式。
    // 最后，返回一个新的 DataFrame，其中的 plan 被更新为 LogicalPlan::Aggregate，表示执行聚合操作。

    #[allow(unused)]
    pub fn aggregate(
        self,
        group_expr: Vec<LogicalExpr>,
        aggr_expr: Vec<AggregateFunction>,
    ) -> Self {
        let mut group_fields = group_expr
            .iter()
            .map(|expr| expr.data_field(&self.plan).unwrap())
            .collect::<Vec<_>>();
        let mut aggr_fields = aggr_expr
            .iter()
            .map(|expr| expr.data_field(&self.plan).unwrap())
            .collect::<Vec<_>>();
        group_fields.append(&mut aggr_fields);
        let schema = NaiveSchema::new(group_fields);
        Self {
            plan: LogicalPlan::Aggregate(Aggregate {
                input: Arc::new(self.plan),
                group_expr,
                aggr_expr,
                schema,
            }),
        }
    }

    pub fn limit(self, n: usize) -> DataFrame {
        Self {
            plan: LogicalPlan::Limit(Limit {
                input: Arc::new(self.plan),
                n,
            }),
        }
    }

    pub fn offset(self, n: usize) -> DataFrame {
        Self {
            plan: LogicalPlan::Offset(Offset {
                input: Arc::new(self.plan),
                n,
            }),
        }
    }

    // join 方法用于执行 连接 操作。它接受三个参数：
    // right：右侧表的 LogicalPlan。
    // join_type：连接类型（如 INNER, LEFT OUTER 等）。
    // join_keys：左表和右表用于连接的列，形式为 (left_keys, right_keys)，分别是左表和右表的列集合。
    // 首先，检查左右连接键的长度是否相等。
    // 如果连接键为空，则执行 交叉连接（CrossJoin），否则执行普通的 连接 操作。
    // 返回一个新的 DataFrame，其中的 plan 被更新为 LogicalPlan::Join 或 LogicalPlan::CrossJoin，表示执行连接操作。
    pub fn join(
        &self,
        right: &LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<Column>, Vec<Column>),
    ) -> Result<DataFrame> {
        if join_keys.0.len() != join_keys.1.len() {
            return Err(ErrorCode::PlanError(
                "left_keys length must be equal to right_keys length".to_string(),
            ));
        }

        let (left_keys, right_keys) = join_keys;
        let on: Vec<(_, _)> = left_keys.into_iter().zip(right_keys.into_iter()).collect();

        let left_schema = self.plan.schema();
        let join_schema = left_schema.join(right.schema());
        // TODO(ywq) test on it.
        if on.is_empty() {
            return Ok(Self::new(LogicalPlan::CrossJoin(Join {
                left: Arc::new(self.plan.clone()),
                right: Arc::new(right.clone()),
                on,
                join_type,
                schema: join_schema,
            })));
        }
        Ok(Self::new(LogicalPlan::Join(Join {
            left: Arc::new(self.plan.clone()),
            right: Arc::new(right.clone()),
            on,
            join_type,
            schema: join_schema,
        })))
    }

    // schema 方法返回当前 DataFrame 的数据模式。是一个 NaiveSchema 类型的引用。
    #[allow(unused)]
    pub fn schema(&self) -> &NaiveSchema {
        self.plan.schema()
    }

    pub fn logical_plan(self) -> LogicalPlan {
        self.plan
    }
}