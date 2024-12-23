use sqlparser::ast::{Assignment, SetExpr, Ident};

use crate::datasource::TableRef;
use crate::logical_plan::expression::{Column, LogicalExpr};

use std::fmt::{Debug, Display, Formatter, Result};
use std::sync::Arc;

use super::expression::AggregateFunction;
use super::schema::NaiveSchema;

#[derive(Clone)]
pub enum LogicalPlan {
    Projection(Projection),

    Filter(Filter),

    #[allow(unused)]

    Aggregate(Aggregate),

    /// Join two logical plans on one or more join columns
    Join(Join),

    CrossJoin(Join),

    /// Produces the first `n` tuples from its input and discards the rest.
    Limit(Limit),

    /// Adjusts the starting point at which the rest of the expressions begin to effect.
    Offset(Offset),

    /// Produces rows from a table provider by reference or from the context
    TableScan(TableScan),

    // 实现将指定元组的属性值更改为指定的新值
    Update(Update),
    // 实现将指定元组插入到表中
    Insert(Insert),
    // 实现将指定元组从表中删除
    Delete(Delete),
    // 实现新建一个元组
    CreateTable(CreateTable),
}

impl LogicalPlan {
    // 返回 LogicalPlan中某个操作的输出模式（即查询结果的结构）。
    // 根据操作类型的不同，schema 方法会递归地调用子计划的 schema 方法，获取最终输出的模式。
    pub fn schema(&self) -> &NaiveSchema {
        match self {
            LogicalPlan::Projection(Projection { schema, .. }) => schema,
            LogicalPlan::Filter(Filter { input, .. }) => input.schema(),
            LogicalPlan::Aggregate(Aggregate { schema, .. }) => schema,
            LogicalPlan::Join(Join { schema, .. }) => schema,
            LogicalPlan::Limit(Limit { input, .. }) => input.schema(),
            LogicalPlan::Offset(Offset { input, .. }) => input.schema(),
            LogicalPlan::TableScan(TableScan { source, .. }) => source.schema(),
            LogicalPlan::CrossJoin(Join { schema, .. }) => schema,
            LogicalPlan::Update(Update { input, .. }) => input.schema(),
            LogicalPlan::Insert(Insert { input, .. }) => input.schema(),
            LogicalPlan::Delete(Delete { input, .. }) => input.schema(),
            LogicalPlan::CreateTable(CreateTable {schema, .. }) => schema
        }
    }
    // 返回当前操作的子计划（输入）。例如，Projection 和 Filter 只有一个输入，
    // Join 需要两个输入。TableScan 没有子计划，因此返回一个空向量。
    #[allow(unused)]
    pub fn children(&self) -> Vec<Arc<LogicalPlan>> {
        match self {
            LogicalPlan::Projection(Projection { input, .. }) => vec![input.clone()],
            LogicalPlan::Filter(Filter { input, .. }) => vec![input.clone()],
            LogicalPlan::Aggregate(Aggregate { input, .. }) => vec![input.clone()],
            LogicalPlan::Join(Join { left, right, .. }) => vec![left.clone(), right.clone()],
            LogicalPlan::Limit(Limit { input, .. }) => vec![input.clone()],
            LogicalPlan::Offset(Offset { input, .. }) => vec![input.clone()],
            LogicalPlan::TableScan(_) => vec![],
            LogicalPlan::CrossJoin(Join { left, right, .. }) => vec![left.clone(), right.clone()],
            LogicalPlan::Update(Update { input, .. }) => vec![input.clone()],
            LogicalPlan::Insert(Insert { input, .. }) => vec![input.clone()],
            LogicalPlan::Delete(Delete { input, .. }) => vec![input.clone()],
            LogicalPlan::CreateTable(_) => vec![]
        }
    }
}

// 实现了对 LogicalPlan 的格式化输出 可以直接在println中输出
impl Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        Debug::fmt(&self, f)
    }
}

impl Debug for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        do_pretty_print(self, f, 0)
    }
}  

#[derive(Debug, Clone)]
pub struct Projection {
    /// The list of expressions
    pub exprs: Vec<LogicalExpr>, 
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The schema description of the output
    pub schema: NaiveSchema,
}

#[derive(Debug, Clone)]
pub struct Filter {
    /// The predicate expression, which must have Boolean type.
    pub predicate: LogicalExpr,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct TableScan {
    /// The source of the table
    pub source: TableRef,
    /// Optional column indices to use as a projection 可选的列索引投影
    pub projection: Option<Vec<usize>>,                // Option<T> 是一个枚举，用于表示一个值可能存在或者不存在。它有两个变体：Some(T) 和 None。Some(T) 表示有一个值，而 None 表示没有值。
}

// lyx 新增 逻辑计划 三个都不需要schema，是因为update、Insert和Delete操作不会改变表的结构，所以不需要schema。
#[derive(Debug, Clone)]
pub struct Update {     // 因为在Filter中已经实现了过滤，所以这里就不需要了
    /// The set of expressions to update (column, value)
    pub assignments: Vec<Assignment>,  // 要更新的列和值
    /// 前面的计划 即一个扫描的
    pub input: Arc<LogicalPlan>,
    pub conditions: LogicalExpr,
}


#[derive(Debug, Clone)]
pub struct Insert {
    pub columns: Vec<Ident>,
    /// The list of expressions representing the values to be inserted
    pub source: SetExpr,  // Values for the new tuple(s)
    /// 前面的计划
    pub input: Arc<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct Delete {
    /// The table to update
    pub source: TableRef,
    /// 前面的计划
    pub input: Arc<LogicalPlan>,
    pub conditions: LogicalExpr,
}

#[derive(Debug, Clone)]
pub struct CreateTable {     // 因为在Filter中已经实现了过滤，所以这里就不需要了
    /// The set of expressions to update (column, value)
    pub table_name: String,
    pub schema: NaiveSchema,
}

/// Aggregates its input based on a set of grouping and aggregate
/// expressions (e.g. SUM).
#[derive(Debug, Clone)]
pub struct Aggregate {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Grouping expressions
    pub group_expr: Vec<LogicalExpr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<AggregateFunction>,
    /// The schema description of the aggregate output
    pub schema: NaiveSchema,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Cross,
}

/// Join two logical plans on one or more join columns
#[derive(Debug, Clone)]
pub struct Join {
    /// Left input
    pub left: Arc<LogicalPlan>,
    /// Right input
    pub right: Arc<LogicalPlan>,
    /// Equijoin clause expressed as pairs of (left, right) join columns, cross join don't have on conditions 连接条件
    pub on: Vec<(Column, Column)>,
    /// Join type
    pub join_type: JoinType,   // 连接类型 内连接，左连接，右连接。。。
    /// The output schema, containing fields from the left and right inputs
    pub schema: NaiveSchema,
}

/// Produces the first `n` tuples from its input and discards the rest.
#[derive(Debug, Clone)]
pub struct Limit {
    /// The limit 限制的行数
    pub n: usize,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
}

/// Adjusts the starting point at which the rest of the expressions begin to effect.
#[derive(Debug, Clone)]
pub struct Offset {
    /// The offset.  跳过查询的行数 与Limit是相反的
    pub n: usize,
    /// The logical plan.
    pub input: Arc<LogicalPlan>,
}

// 通过递归调用来打印每个操作的详细信息，并根据不同的操作类型格式化输出。
fn do_pretty_print(plan: &LogicalPlan, f: &mut Formatter<'_>, depth: usize) -> Result {
    write!(f, "{}", "  ".repeat(depth))?;

    match plan {
        LogicalPlan::CreateTable(CreateTable { table_name, schema }) => {
            writeln!(f, "CreateTable:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "table_name: {}", table_name)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "schema:")?;
            // Print the schema (fields of the table)
            for field in &schema.fields {
                write!(f, "{}", "  ".repeat(depth + 2))?;
                writeln!(f, "field: {}", field.name())?;
            }
            Ok(())
        }
        LogicalPlan::Projection(Projection {
            exprs,
            input,
            schema,
        }) => {
            writeln!(f, "Projection:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "exprs: {:?}", exprs)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "input:")?;
            do_pretty_print(input.as_ref(), f, depth + 2)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "schema: {:?}", schema)
        }
        LogicalPlan::Delete(Delete {
            source,
            input,
            conditions,
        }) => {
            writeln!(f, "Delete:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "source: {:?}", source.source_name())?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "conditions: {:?}", conditions)?;

            // Print the input plan (previous logical plan)
            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "input:")?;
            do_pretty_print(input.as_ref(), f, depth + 2)
        }
        LogicalPlan::Insert(Insert {
            columns,
            source,
            input,
        }) => {
            writeln!(f, "Insert:")?;
            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "columns: {:?}", columns)?;

            // Print source (values or query)
            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "source:")?;
            match source {
                SetExpr::Values(values) => {
                    write!(f, "{}", "  ".repeat(depth + 2))?;
                    writeln!(f, "Values: {:?}", values)?;
                }
                _ => {
                    write!(f, "{}", "  ".repeat(depth + 2))?;
                    writeln!(f, "Other source type: {:?}", source)?;
                }
            }

            // Print the input plan (previous logical plan)
            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "input:")?;
            do_pretty_print(input.as_ref(), f, depth + 2)
        }
        LogicalPlan::Update(Update {
            conditions,
            assignments,
            input,
        }) => {
            writeln!(f, "Update:")?;
            // Print assignments (columns and their new values)
            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "assignments:")?;
            for assignment in assignments {
                write!(f, "{}", "  ".repeat(depth + 2))?;
                writeln!(f, "column: {:?}, value: {:?}", assignment.id, assignment.value)?;
            }

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "conditions: {:?}", conditions)?;

            // Print the input plan (previous logical plan)
            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "input:")?;
            do_pretty_print(input.as_ref(), f, depth + 2)
        }
        LogicalPlan::Filter(Filter { predicate, input }) => {
            writeln!(f, "Filter:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "predicate: {:?}", predicate)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "input:")?;
            do_pretty_print(input.as_ref(), f, depth + 2)
        }
        LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
        }) => {
            writeln!(f, "Aggregate:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "input:")?;
            do_pretty_print(input.as_ref(), f, depth + 2)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "group_expr: {:?}", group_expr)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "aggr_expr: {:?}", aggr_expr)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "schema: {:?}", schema)
        }
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            join_type,
            schema,
        }) => {
            writeln!(f, "Join:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "left:")?;
            do_pretty_print(left.as_ref(), f, depth + 2)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "right:")?;
            do_pretty_print(right.as_ref(), f, depth + 2)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "on: {:?}", on)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "join_type: {:?}", join_type)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "schema: {:?}", schema)
        }
        LogicalPlan::Limit(Limit { n, input }) => {
            writeln!(f, "Limit:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "n: {}", n)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "input:")?;
            do_pretty_print(input.as_ref(), f, depth + 2)
        }
        LogicalPlan::Offset(Offset { n, input }) => {
            writeln!(f, "Offset:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "n: {}", n)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "input:")?;
            do_pretty_print(input.as_ref(), f, depth + 2)
        }
        LogicalPlan::TableScan(TableScan { source, projection }) => {
            writeln!(f, "TableScan:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "source: {:?}", source.source_name())?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "projection: {:?}", projection)
        }
        LogicalPlan::CrossJoin(Join {
            left,
            right,
            on: _,
            join_type,
            schema,
        }) => {
            writeln!(f, "Join:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "left:")?;
            do_pretty_print(left.as_ref(), f, depth + 2)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "right:")?;
            do_pretty_print(right.as_ref(), f, depth + 2)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "join_type: {:?}", join_type)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "schema: {:?}", schema)
        }
    }
}

