use std::iter::repeat;

use arrow::array::StringArray;
use arrow::array::{new_null_array, ArrayRef, BooleanArray, Float64Array, Int64Array, UInt64Array};

use arrow::datatypes::DataType;
use std::sync::Arc;

use crate::error::{ErrorCode, Result};

use crate::logical_plan::plan::LogicalPlan;

use super::schema::NaiveField;

/// 实现将statemnt中的Expr转换为LogicalExpr逻辑表达式类型
#[derive(Clone, Debug)]
pub enum LogicalExpr {
    #[allow(unused)]
    // 表达式的别名
    Alias(Box<LogicalExpr>, String),    
    // 列，包含字段表名和字段名
    Column(Column),
    // 常量
    Literal(ScalarValue),
    // 二元表达式
    BinaryExpr(BinaryExpr),

    #[allow(unused)]
    // 聚合函数
    AggregateFunction(AggregateFunction),
    // 通配符，表示所有字段
    Wildcard,
}

impl LogicalExpr {
    // 生成一个列/字段
    pub fn column(table: Option<String>, name: String) -> LogicalExpr {
        LogicalExpr::Column(Column { table, name })
    }

    // 从一个逻辑计划中提取出字段的信息，根据不同的表达式类型返回相应的字段定义
    pub fn data_field(&self, input: &LogicalPlan) -> Result<NaiveField> {
        match self {    // 匹配不同的逻辑表达式LogicalRxpr执行不同的操作
            LogicalExpr::Alias(expr, alias) => {
                let field = expr.data_field(input)?;
                Ok(NaiveField::new(
                    None,
                    alias,
                    field.data_type().clone(),
                    field.is_nullable(),
                ))
            }
            LogicalExpr::Column(Column { name, table }) => match table {
                Some(table) => input.schema().field_with_qualified_name(table, name),
                None => input.schema().field_with_unqualified_name(name),
            },
            LogicalExpr::Literal(scalar_val) => Ok(scalar_val.data_field()),
            LogicalExpr::BinaryExpr(expr) => expr.data_field(input),
            LogicalExpr::AggregateFunction(aggr_func) => aggr_func.data_field(input),
            LogicalExpr::Wildcard => Err(ErrorCode::IntervalError(
                "Wildcard not supported in logical plan".to_string(),
            )),
        }
    }

    // 将当前表达式和另一个表达式通过逻辑与操作符连接，生成一个新的二元表达式
    pub fn and(self, other: LogicalExpr) -> LogicalExpr {
        binary_expr(self, Operator::And, other)
    }

    // 🌟创建聚合函数 支持 count、sum、avg、min、max 
    pub fn try_create_aggregate_func(
        func_name: &str,    
        exprs: &[LogicalExpr],
    ) -> Result<LogicalExpr> {
        if exprs.len() != 1 {
            return Err(ErrorCode::PlanError(
                "Aggregate Func Now only Support One parameter".to_string(),
            ));
        }
        match func_name {
            "count" => Ok(LogicalExpr::AggregateFunction(AggregateFunction {
                fun: AggregateFunc::Count,
                args: Box::new(exprs[0].clone()),
            })),
            "sum" => Ok(LogicalExpr::AggregateFunction(AggregateFunction {
                fun: AggregateFunc::Sum,
                args: Box::new(exprs[0].clone()),
            })),
            "avg" => Ok(LogicalExpr::AggregateFunction(AggregateFunction {
                fun: AggregateFunc::Avg,
                args: Box::new(exprs[0].clone()),
            })),
            "min" => Ok(LogicalExpr::AggregateFunction(AggregateFunction {
                fun: AggregateFunc::Min,
                args: Box::new(exprs[0].clone()),
            })),
            "max" => Ok(LogicalExpr::AggregateFunction(AggregateFunction {
                fun: AggregateFunc::Max,
                args: Box::new(exprs[0].clone()),
            })),
            _ => {
                return Err(ErrorCode::NoMatchFunction(format!(
                    "Not match aggregate func: {}",
                    func_name
                )));
            }
        }
    }
}

// 二元表达式 l <op> r
pub fn binary_expr(l: LogicalExpr, op: Operator, r: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryExpr(BinaryExpr {
        left: Box::new(l),
        op,
        right: Box::new(r),
    })
}

// 列的别名设置
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Column {
    pub table: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone)]

pub enum ScalarValue {     // 常量类型枚举
    Null,
    Boolean(Option<bool>),
    Float64(Option<f64>),
    Int64(Option<i64>),
    UInt64(Option<u64>),
    Utf8(Option<String>),
}

macro_rules! build_array_from_option {
    ($DATA_TYPE:ident, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => Arc::new($ARRAY_TYPE::from_value(value, $SIZE)),
            None => new_null_array(&DataType::$DATA_TYPE, $SIZE),
        }
    }};
}

impl ScalarValue {
    pub fn data_field(&self) -> NaiveField {
        match self {
            ScalarValue::Null => NaiveField::new(None, "Null", DataType::Null, true),
            ScalarValue::Boolean(_) => NaiveField::new(None, "bool", DataType::Boolean, true),
            ScalarValue::Float64(_) => NaiveField::new(None, "f64", DataType::Float64, true),
            ScalarValue::Int64(_) => NaiveField::new(None, "i64", DataType::Int64, true),
            ScalarValue::UInt64(_) => NaiveField::new(None, "u64", DataType::UInt64, true),
            ScalarValue::Utf8(_) => NaiveField::new(None, "string", DataType::Utf8, true),
        }
    }

    pub fn into_array(self, size: usize) -> ArrayRef {
        match self {
            ScalarValue::Null => new_null_array(&DataType::Null, size),
            ScalarValue::Boolean(e) => Arc::new(BooleanArray::from(vec![e; size])) as ArrayRef,
            ScalarValue::Float64(e) => build_array_from_option!(Float64, Float64Array, e, size),
            ScalarValue::Int64(e) => build_array_from_option!(Int64, Int64Array, e, size),
            ScalarValue::UInt64(e) => build_array_from_option!(UInt64, UInt64Array, e, size),
            ScalarValue::Utf8(e) => match e {
                Some(value) => Arc::new(StringArray::from_iter_values(repeat(value).take(size))),
                None => new_null_array(&DataType::Utf8, size),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    pub left: Box<LogicalExpr>,
    pub op: Operator,
    pub right: Box<LogicalExpr>,
}

// 二元表达式
impl BinaryExpr {
    pub fn data_field(&self, input: &LogicalPlan) -> Result<NaiveField> {
        let left = self.left.data_field(input)?;
        let left = left.name();
        let right = match &*self.right {
            LogicalExpr::Literal(scalar_val) => match scalar_val {
                ScalarValue::Boolean(Some(val)) => val.to_string(),
                ScalarValue::Int64(Some(val)) => val.to_string(),
                ScalarValue::UInt64(Some(val)) => val.to_string(),
                ScalarValue::Float64(Some(val)) => val.to_string(),
                ScalarValue::Utf8(Some(val)) => val.to_string(),
                _ => "null".to_string(),
            },
            _ => self.right.data_field(input)?.name().clone(),
        };
        let field = match self.op {
            Operator::Eq => NaiveField::new(
                None,
                format!("{} = {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::NotEq => NaiveField::new(
                None,
                format!("{} != {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::Lt => NaiveField::new(
                None,
                format!("{} < {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::LtEq => NaiveField::new(
                None,
                format!("{} <= {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::Gt => NaiveField::new(
                None,
                format!("{} > {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::GtEq => NaiveField::new(
                None,
                format!("{} >= {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::Plus => NaiveField::new(
                None,
                format!("{} + {}", left, right).as_str(),
                self.left.data_field(input)?.data_type().clone(),
                true,
            ),
            Operator::Minus => NaiveField::new(
                None,
                format!("{} - {}", left, right).as_str(),
                self.left.data_field(input)?.data_type().clone(),
                true,
            ),
            Operator::Multiply => NaiveField::new(
                None,
                format!("{} * {}", left, right).as_str(),
                self.left.data_field(input)?.data_type().clone(),
                true,
            ),
            Operator::Divide => NaiveField::new(
                None,
                format!("{} / {}", left, right).as_str(),
                self.left.data_field(input)?.data_type().clone(),
                true,
            ),
            Operator::Modulos => NaiveField::new(
                None,
                format!("{} % {}", left, right).as_str(),
                self.left.data_field(input)?.data_type().clone(),
                true,
            ),
            Operator::And => NaiveField::new(
                None,
                format!("{} and {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::Or => NaiveField::new(
                None,
                format!("{} or {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
        };
        Ok(field)
    }
}

#[derive(Debug, Clone)]
pub enum Operator {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is smaller than right side
    Lt,
    /// Left side is smaller or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater or equal to right side
    GtEq,
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication operator, like `*`
    Multiply,
    /// Division operator, like `/`
    Divide,
    /// Remainder operator, like `%`
    Modulos,
    /// Logical AND, like `&&`
    And,
    /// Logical OR, like `||`
    Or,
}


#[derive(Debug, Clone)]
pub struct AggregateFunction {
    /// Name of the function
    pub fun: AggregateFunc,
    /// List of expressions to feed to the functions as arguments
    pub args: Box<LogicalExpr>,
}

impl AggregateFunction {
    pub fn data_field(&self, input: &LogicalPlan) -> Result<NaiveField> {
        let dt = self.args.data_field(input)?;
        let field = match self.fun {
            AggregateFunc::Count => NaiveField::new(
                None,
                format!("count({})", dt.name()).as_str(),
                dt.data_type().clone(),
                true,
            ),
            AggregateFunc::Sum => NaiveField::new(
                None,
                format!("sum({})", dt.name()).as_str(),
                dt.data_type().clone(),
                true,
            ),
            AggregateFunc::Min => NaiveField::new(
                None,
                format!("min({})", dt.name()).as_str(),
                dt.data_type().clone(),
                true,
            ),
            AggregateFunc::Max => NaiveField::new(
                None,
                format!("max({})", dt.name()).as_str(),
                dt.data_type().clone(),
                true,
            ),
            AggregateFunc::Avg => NaiveField::new(
                None,
                format!("avg({})", dt.name()).as_str(),
                dt.data_type().clone(),
                true,
            ),
        };
        Ok(field)
    }
}

#[derive(Debug, Clone)]
pub enum AggregateFunc {
    #[allow(unused)]
    Count,
    #[allow(unused)]
    Sum,
    #[allow(unused)]
    Min,
    #[allow(unused)]
    Max,
    #[allow(unused)]
    Avg,
}
