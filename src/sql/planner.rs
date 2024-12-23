use std::collections::HashSet;

use crate::logical_plan::schema::NaiveField;
use sqlparser::ast::ColumnDef;
use arrow::datatypes::DataType as ArrowDataType;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, Join, JoinConstraint, JoinOperator, SetExpr,
    Statement, TableWithJoins, Assignment,     
};
use sqlparser::ast::Offset;
use sqlparser::ast::{Ident, ObjectName, SelectItem, TableFactor, Value};
use sqlparser::ast::ColumnOption;

use crate::error::ErrorCode;
use crate::logical_plan::expression::{
    BinaryExpr, Column, LogicalExpr, Operator, ScalarValue,
};
use crate::logical_plan::literal::lit;
use crate::logical_plan::plan::{JoinType, TableScan, CreateTable};

use crate::logical_plan::schema::NaiveSchema;
use crate::{
    catalog::Catalog,
    error::Result,
    logical_plan::{plan::LogicalPlan, DataFrame},
};

// SQLPlanner: convert statement to logical plan
pub struct SQLPlanner<'a> {
    catalog: &'a Catalog,   // 引用一个 Catalog，用来管理数据库中的表和视图。
}
 
impl<'a> SQLPlanner<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    // ****执行update语句的时候会卡在这里 说明update语句是没有实现的 
    pub fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        match statement {      // match匹配语句
            // -----select语句-----
            Statement::Query(query) => {      // 明确的匹配模式
                let plan = self.set_expr_to_plan(query.body)?;   
                // 首先执行offset，再执行limit
                let plan = self.offset(plan, query.offset)?;
                self.limit(plan, query.limit)
            }

            // -----create语句-----  name cloumns 重点需要考虑的三个变量 暂时没考虑约束
            Statement::CreateTable{or_replace:_,temporary:_, external:_, if_not_exists:_, name,columns,constraints:_, hive_distribution:_, hive_formats:_, table_properties:_, with_options:_, file_format:_, location:_, query:_, without_rowid:_, like:_} => {
                let table_name = Self::normalize_sql_object_name(&name);
                let schema = Self::columns_to_naive_schema(&columns);
                // 处理其他的参数，将其组装到一个查询计划中
                self.plan_create(table_name, schema)
            }

            // -----drop语句----- 
            Statement::Drop{object_type:_, if_exists:_, names, cascade:_, purge:_} => {   
                self.parse_table_new(&names[0])
            }

            // -----update语句----- 
            Statement::Update{table_name, assignments, selection } => {   
                // 1. 处理表名 这里只可能会涉及一个表
                let plan = self.parse_table_new(&table_name)?;
                // 2. 不能提前 处理WHERE条件 因为会使得最终结果只有修改后的元组 所以我们将其均组装到一起
                self.plan_update_assignments(selection, assignments, plan)
            }
            
            
            // -----insert语句-----    主要组成部分 1. INTO：指定要插入数据的表名 2. VALUES：指定要插入的数据
            // INSERT INTO table_name (column1, column2) VALUES (value1, value2);
            Statement::Insert{or:_, table_name, columns, overwrite:_, source, partitioned:_, after_columns:_, table:_} => {
                // 1. 处理表名 这里只可能会涉及一个表
                let plan = self.parse_table_new(&table_name)?; 
                // 2. 执行插入
                self.plan_insert(columns,source.body, plan)
            }

            // -----delete语句-----     主要组成部分 1. FROM：指定要删除的表 2. WHERE：指定删除的条件
            // DELETE FROM table_name WHERE condition;
            Statement::Delete{table_name, selection} => {
                // 1. 处理删除的目标表
                let plan = self.parse_table_new(&table_name)?;

                // 2. 和update操作同理，不能提前处理where过滤条件
                self.plan_delete(&table_name, selection, plan)
            }

            _ => unimplemented!(),    // 通配符匹配模式，最初用来捕获所有不属于上述statement值 表明我们还没有实现😭
        }
    }

    // 传入的是query_body，是select的主体部分，SetExpr类型，包含select的各种子句
    fn set_expr_to_plan(&self, set_expr: SetExpr) -> Result<LogicalPlan> {
        match set_expr {
            // 匹配第一个部分Select(Box<Select>)
            SetExpr::Select(select) => {
                let plans = self.plan_from_tables(select.from)?;   // 将1.表及其2.连接关系解析为LogicalPlan

                let plan = self.plan_selection(select.selection, plans)?;  // where语句的处理，筛选符合条件的行

                let select_exprs = self.prepare_select_exprs(&plan, &select.projection)?; 
                // filter aggregate expr, these exps should not pass to projection
                let aggr_exprs_haystack = select_exprs;
                let (aggr_exprs, project_exprs) = self.find_agrr_exprs(&aggr_exprs_haystack);
                let plan = if aggr_exprs.is_empty() {
                    plan
                } else {
                    self.plan_from_aggregate(plan, aggr_exprs, select.group_by)?    
                };

                // process the SELECT expressions, with wildcards expanded
                let plan = self.plan_from_projection(plan, project_exprs)?;

                Ok(plan)
            }
            _ => todo!(),
        }
    }


    fn plan_from_aggregate(
        &self,
        plan: LogicalPlan,
        aggr_exprs: Vec<LogicalExpr>,
        group_by: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let mut group_by_exprs = vec![];
        for expr in &group_by {
            group_by_exprs.push(self.sql_to_expr(expr)?);
        }

        let mut aggr_func = vec![];
        for aggr_expr in &aggr_exprs {
            if let LogicalExpr::AggregateFunction(aggr) = aggr_expr {
                aggr_func.push(aggr.clone());
            }
        }

        let df = DataFrame::new(plan);
        Ok(df.aggregate(group_by_exprs, aggr_func).logical_plan())
    }

    fn find_agrr_exprs(&self, exprs: &[LogicalExpr]) -> (Vec<LogicalExpr>, Vec<LogicalExpr>) {
        let mut aggr_exprs = vec![];    // 聚合函数列
        let mut project_exprs = vec![]; // 普通列
        for expr in exprs {
            match expr {
                LogicalExpr::AggregateFunction(_) => aggr_exprs.push(expr.clone()),
                _ => project_exprs.push(expr.clone()),
            }
        }
        (aggr_exprs, project_exprs)
    }

    fn prepare_select_exprs(
        &self,
        plan: &LogicalPlan,
        projection: &[SelectItem],
    ) -> Result<Vec<LogicalExpr>> {
        let input_schema = plan.schema();

        Ok(projection
            .iter()
            .map(|expr| self.select_item_to_expr(expr))
            .collect::<Result<Vec<LogicalExpr>>>()?
            .iter()
            .flat_map(|expr| Self::expand_wildcard(expr, input_schema))
            .collect::<Vec<LogicalExpr>>())
    }

    /// Generate a relational expression from a select SQL expression
    fn select_item_to_expr(&self, sql: &SelectItem) -> Result<LogicalExpr> {
        match sql {
            SelectItem::UnnamedExpr(expr) => self.sql_to_expr(expr),
            SelectItem::Wildcard => Ok(LogicalExpr::Wildcard),
            _ => unimplemented!(),
        }
    }

    fn expand_wildcard(expr: &LogicalExpr, schema: &NaiveSchema) -> Vec<LogicalExpr> {
        match expr {
            LogicalExpr::Wildcard => schema
                .fields()
                .iter()
                .map(|f| LogicalExpr::column(None, f.name().to_string()))
                .collect::<Vec<LogicalExpr>>(),
            _ => vec![expr.clone()],
        }
    }

    // 实现limit 指定返回的行数
    fn limit(&self, plan: LogicalPlan, limit: Option<Expr>) -> Result<LogicalPlan> {
        match limit {
            Some(limit_expr) => {
                let n = match self.sql_to_expr(&limit_expr)? {
                    LogicalExpr::Literal(ScalarValue::Int64(Some(n))) => Ok(n as usize),
                    _ => Err(ErrorCode::PlanError(
                        "Unexpected expression for LIMIT clause".to_string(),
                    )),
                }?;
                Ok(DataFrame { plan }.limit(n).logical_plan())
            }
            None => Ok(plan),
        }
    }

    // 实现offset 指定跳过的行数
    fn offset(&self, plan: LogicalPlan, offset: Option<Offset>) -> Result<LogicalPlan> {
        match offset {
            Some(offset) => {
                let n = match self.sql_to_expr(&offset.value)? {
                    LogicalExpr::Literal(ScalarValue::Int64(Some(n))) => Ok(n as usize),
                    _ => Err(ErrorCode::PlanError(
                        "Unexpected expression for Offset clause".to_string(),
                    )),
                }?;
                Ok(DataFrame { plan }.offset(n).logical_plan())
            }
            None => Ok(plan),
        }
    }

    /* From子句的入口函数及核心处理 可能需要处理TableWithJoins表示的一个表/多表连接关系 */
    // from 向量的长度为 0，表示 SQL 查询没有指定任何表。这时，代码使用 todo!() 触发一个占位符错误，并提示未实现该功能的情况。
    // from 向量的长度大于 0，代码会遍历 from 中的每一个 TableWithJoins（即每个表及其可能存在连接），
    // 并调用 self.plan_table_with_joins(t) 方法来生成每个表的逻辑计划。
    // 最终，使用 collect 将所有生成的逻辑计划收集到一个向量中，返回一个 Result<Vec<LogicalPlan>>。
    fn plan_from_tables(&self, from: Vec<TableWithJoins>) -> Result<Vec<LogicalPlan>> {
        match from.len() {    
            0 => todo!("support select with no from"),   
            _ => from
                .iter()
                .map(|t| self.plan_table_with_joins(t))
                .collect::<Result<Vec<_>>>(),
        }
    }

    // 输入是一个包含表信息和连接信息的结构体 递归实现连接 逻辑计划最终存储在left中
    fn plan_table_with_joins(&self, t: &TableWithJoins) -> Result<LogicalPlan> {
        let left = self.parse_table(&t.relation)?;  // 解析表的基本信息
        match t.joins.len() {
            0 => Ok(left),    // 没有Join则直接返回left的LogicalPlan 
            n => {     // 有Join则递归处理每个连接 即把每次join的结果存储在left中，递归实现join 
                let mut left = self.parse_table_join(left, &t.joins[0])?;
                for i in 1..n {
                    left = self.parse_table_join(left, &t.joins[i])?;
                }
                Ok(left)
            }
        }
    }

    // 调用 parse_table 解析右表（right）。
    // 根据连接类型（JoinOperator），调用相应的 parse_join 函数来生成连接操作的逻辑计划。
    // 包括四种连接类型 分别是：LeftOuter, RightOuter, Inner, CrossJoin。
    fn parse_table_join(&self, left: LogicalPlan, join: &Join) -> Result<LogicalPlan> {
        let right = self.parse_table(&join.relation)?;   // 解析连接表的信息，传递给parse join函数进行处理
        match &join.join_operator {
            JoinOperator::LeftOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Left)
            }
            JoinOperator::RightOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Right)
            }
            JoinOperator::Inner(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Inner)
            }
            JoinOperator::CrossJoin => {
                self.parse_join(left, right, &JoinConstraint::None, JoinType::Cross)
            }

            _other => Err(ErrorCode::NotImplemented),
        }
    }

    // 生成实际的连接操作计划。
    // 它根据连接的类型和约束（例如 ON 条件或无条件连接）来生成连接的逻辑计划。
    // 如果连接有 ON 条件（JoinConstraint::On），解析条件，提取连接的键和值，并生成过滤器。然后执行连接操作。
    // 如果没有连接条件（JoinConstraint::None），直接执行连接。
    // 如果是 INNER JOIN，处理过滤条件并生成最终的逻辑计划。   明确这里新建的DataFrame是什么含义
    fn parse_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        constraint: &JoinConstraint,     // 连接条件 即on子句的表达式
        join_type: JoinType,    // 连接类型 包括Inner, Left, Right, Cross
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(sql_expr) => {
                let mut keys: Vec<(Column, Column)> = vec![];   //  存储连接键的向量
                let expr = self.sql_to_expr(sql_expr)?;  // 将 SQL 表达式转换为逻辑表达式

                let mut filters = vec![];
                extract_join_keys(&expr, &mut keys, &mut filters);   // 从表达式中提取键和值

                let left_keys = keys.iter().map(|pair| pair.0.clone()).collect();
                let right_keys = keys.iter().map(|pair| pair.1.clone()).collect();

                if filters.is_empty() {    // 无过滤条件 直接执行连接条件
                    let join =
                        DataFrame::new(left).join(&right, join_type, (left_keys, right_keys))?;
                    Ok(join.logical_plan())
                } else if join_type == JoinType::Inner {   // 有过滤条件 且是 INNER JOIN 说明当前只实现了InnerJoin
                    let join =
                        DataFrame::new(left).join(&right, join_type, (left_keys, right_keys))?;
                    let join = join.filter(     // 使用 filter 方法将过滤条件应用到连接结果上 使用fold函数将多个过滤条件合并在一起
                        filters
                            .iter()
                            .skip(1)
                            .fold(filters[0].clone(), |acc, e| acc.and(e.clone())),
                    );
                    Ok(join.logical_plan())
                } else {
                    Err(ErrorCode::NotImplemented)    // 当前只实现了InnerJoin的方式
                }
            }    // 如果没有连接条件 即不存在on 直接进行连接操作，left_keys 和 right_keys 都为空
            JoinConstraint::None => {
                let join = DataFrame::new(left).join(&right, join_type, (vec![], vec![]))?;
                Ok(join.logical_plan())
            }
            _ => Err(ErrorCode::NotImplemented),
        }
    }

    // 解析单个表的基本信息，生成对应的 LogicalPlan。
    fn parse_table(&self, relation: &TableFactor) -> Result<LogicalPlan> {
        match &relation {
            TableFactor::Table { name, .. } => {
                let table_name = Self::normalize_sql_object_name(name);
                let source = self.catalog.get_table(&table_name)?;
                Ok(LogicalPlan::TableScan(TableScan {
                    source,
                    projection: None,
                }))
            }
            _ => unimplemented!(),
        }
    }

    // 除了select以外 其他查询语句都需要使用这个 因为传入的参数有所不同
    fn parse_table_new(&self, name: &ObjectName) -> Result<LogicalPlan> {
        let table_name = Self::normalize_sql_object_name(name);
        let source = self.catalog.get_table(&table_name)?;
        let plan = LogicalPlan::TableScan(TableScan {
            source,
            projection: None,
        });
    
        // 返回一个包含单个逻辑计划的向量
        Ok(plan)
    }

    // ---update专属---=
    fn plan_update_assignments(
        &self, 
        selection: Option<Expr>,
        assignments: Vec<Assignment>, 
        plan: LogicalPlan
    ) -> Result<LogicalPlan> {
        let df = DataFrame::new(plan);
        match selection {
            Some(expr) => {
                let conditions = self.sql_to_expr(&expr)?;
                Ok(df.update(conditions, assignments)?.logical_plan())
            }
            None => {
                Err(ErrorCode::NotImplemented)
            }
        }
    }
    
    // ---createTable专属---
    pub fn columns_to_naive_schema(columns: &Vec<ColumnDef>) -> NaiveSchema {
        let fields: Vec<NaiveField> = columns
            .iter()
            .map(|column| {
                let data_type = match &column.data_type {
                    sqlparser::ast::DataType::Boolean => ArrowDataType::Boolean,
                    sqlparser::ast::DataType::Int => ArrowDataType::Int64,
                    sqlparser::ast::DataType::Varchar(_) => ArrowDataType::Utf8,
                    sqlparser::ast::DataType::Float(_) => ArrowDataType::Float64,
                    sqlparser::ast::DataType::Decimal(_, _) => ArrowDataType::Decimal(10, 2), // 假设为10,2精度
                    _ => ArrowDataType::Utf8, // 默认类型为 Utf8
                };
                let nullable = column.options.iter().any(|opt| matches!(opt.option, ColumnOption::Null));
                let name = column.name.to_string();
                NaiveField::new(None, &name, data_type, nullable)
            })
            .collect();
    
        NaiveSchema::new(fields)
    }

    // ---createTable专属---
    fn plan_create(
        &self, 
        table_name: String,
        schema: NaiveSchema,
    ) -> Result<LogicalPlan> {
        Ok(LogicalPlan::CreateTable(CreateTable {
            table_name,
            schema,
        }))
    }

    fn plan_insert(
        &self, 
        columns: Vec<Ident>, 
        source: SetExpr,
        plan: LogicalPlan
    ) -> Result<LogicalPlan> {
        let df = DataFrame::new(plan);
        Ok(df.insert(columns, source)?.logical_plan())
    }

    fn plan_delete(
        &self, 
        table_name: &ObjectName,
        selection: Option<Expr>,
        plan: LogicalPlan
    ) -> Result<LogicalPlan> {
        let name = Self::normalize_sql_object_name(table_name);
        let source = self.catalog.get_table(&name)?;
        let df = DataFrame::new(plan);
        match selection {
            Some(expr) => {
                let conditions = self.sql_to_expr(&expr)?;
                Ok(df.delete(source, conditions)?.logical_plan())
            }
            None => {
                Err(ErrorCode::NotImplemented)
            }
        }
    }

    fn plan_from_projection(
        &self,
        plan: LogicalPlan,
        projection: Vec<LogicalExpr>,
    ) -> Result<LogicalPlan> {
        let df = DataFrame::new(plan);
        Ok(df.project(projection)?.logical_plan())
    }

    // 对于Where子句的处理，执行关联操作或直接返回符合条件的逻辑计划。
    fn plan_selection(
        &self,
        selection: Option<Expr>,
        plans: Vec<LogicalPlan>,
    ) -> Result<LogicalPlan> {
        match selection {
            Some(expr) => {
                let mut fields = vec![];
                for plan in &plans {
                    fields.extend_from_slice(plan.schema().fields());
                }
                let filter_expr = self.sql_to_expr(&expr)?;

                // look for expressions of the form `<column> = <column>`
                let mut possible_join_keys = vec![];
                extract_possible_join_keys(&filter_expr, &mut possible_join_keys)?;

                let mut all_join_keys = HashSet::new();
                let mut left = plans[0].clone();
                for right in plans.iter().skip(1) {
                    let left_schema = left.schema();
                    let right_schema = right.schema();
                    let mut join_keys = vec![];
                    for (l, r) in &possible_join_keys {
                        if left_schema
                            .field_with_unqualified_name(l.name.as_str())
                            .is_ok()
                            && right_schema
                                .field_with_unqualified_name(r.name.as_str())
                                .is_ok()
                        {
                            join_keys.push((l.clone(), r.clone()));
                        } else if left_schema
                            .field_with_unqualified_name(r.name.as_str())
                            .is_ok()
                            && right_schema
                                .field_with_unqualified_name(l.name.as_str())
                                .is_ok()
                        {
                            join_keys.push((r.clone(), l.clone()));
                        }
                    }
                    if !join_keys.is_empty() {
                        let left_keys: Vec<Column> =
                            join_keys.iter().map(|(l, _)| l.clone()).collect();
                        let right_keys: Vec<Column> =
                            join_keys.iter().map(|(_, r)| r.clone()).collect();
                        let df = DataFrame::new(left);
                        left = df
                            .join(right, JoinType::Inner, (left_keys, right_keys))?
                            .logical_plan();
                    } else {
                        return Err(ErrorCode::NotImplemented);
                    }

                    all_join_keys.extend(join_keys);
                }
                // remove join expressions from filter
                match remove_join_expressions(&filter_expr, &all_join_keys)? {
                    Some(filter_expr) => {
                        Ok(DataFrame::new(left).filter(filter_expr).logical_plan())
                    }
                    _ => Ok(left),
                }
            }
            None => {
                if plans.len() == 1 {
                    Ok(plans[0].clone())
                } else {
                    Err(ErrorCode::NotImplemented)
                }
            }
        }
    }
    

    /// 将parser解析得到的ObjectName类型的表名转换成String类型的名称
    fn normalize_sql_object_name(sql_object_name: &ObjectName) -> String {
        sql_object_name
            .0
            .iter()
            .map(normalize_ident)
            .collect::<Vec<String>>()
            .join(".")
    }

    // 将SQL语句转换成逻辑表达式 🌟  输入 是一个SQL表达式 sqlparser::ast::Expr 类型 输出 是LogicalExpr: 表示逻辑计划的表达式，支持各种操作符、常量、函数等。
    // 函数 sql_to_expr 将 sqlparser 的 Expr 类型转化为自定义的 LogicalExpr，使 SQL 查询可以被内部的查询引擎逻辑理解和处理。
    fn sql_to_expr(&self, sql: &Expr) -> Result<LogicalExpr> {
        match sql {
            Expr::Value(Value::Boolean(n)) => Ok(lit(*n)), // 布尔值
            Expr::Value(Value::Number(n, _)) => match n.parse::<i64>() {
                Ok(n) => Ok(lit(n)),   // 数值解析为i64
                Err(_) => Ok(lit(n.parse::<f64>().unwrap())),   // 否则尝试解析为f64
            },
            Expr::Value(Value::SingleQuotedString(ref s)) => Ok(lit(s.clone())), // 单引号字符串值
            Expr::Value(Value::Null) => Ok(LogicalExpr::Literal(ScalarValue::Null)),   
            // 单个标识符（例如列名 id）被转换为 LogicalExpr::column，表示逻辑计划中的列。
            Expr::Identifier(id) => Ok(LogicalExpr::column(None, normalize_ident(id))),

            // 二元操作符
            Expr::BinaryOp { left, op, right } => self.parse_sql_binary_op(left, op, right),
            // 复合标识符 支持带表名的列（如 table.column）
            Expr::CompoundIdentifier(ids) => {
                let mut var_names = ids.iter().map(|id| id.value.clone()).collect::<Vec<_>>();

                match (var_names.pop(), var_names.pop()) {
                    (Some(name), Some(table)) if var_names.is_empty() => {
                        // table.column identifier
                        Ok(LogicalExpr::Column(Column {
                            table: Some(table),
                            name,
                        }))
                    }
                    _ => Err(ErrorCode::NotImplemented),
                }
            }
            // 函数调用
            Expr::Function(function) => {
                let name = if !function.name.0.is_empty() {
                    function.name.to_string()
                } else {
                    return Err(ErrorCode::PlanError(
                        "Function not support with quote".to_string(),
                    ));
                };

                // 计算参数 递归调用 sql_to_expr 解析参数
                let mut args = vec![];
                for arg in &function.args {
                    let arg = match arg {
                        FunctionArg::Named { name: _, arg } => self.sql_to_expr(arg),
                        FunctionArg::Unnamed(arg) => self.sql_to_expr(arg),
                    }?;
                    args.push(arg);
                }


                // 聚合函数
                if let Ok(func) = LogicalExpr::try_create_aggregate_func(&name, &args) {
                    return Ok(func);
                };

                Err(ErrorCode::NoMatchFunction(format!(
                    "Not find match func: {}",
                    name
                )))
            }
            _ => todo!(),
        }
    }

    fn parse_sql_binary_op(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<LogicalExpr> {
        let op = match op {
            BinaryOperator::Eq => Operator::Eq,
            BinaryOperator::NotEq => Operator::NotEq,
            BinaryOperator::Lt => Operator::Lt,
            BinaryOperator::LtEq => Operator::LtEq,
            BinaryOperator::Gt => Operator::Gt,
            BinaryOperator::GtEq => Operator::GtEq,
            BinaryOperator::Plus => Operator::Plus,
            BinaryOperator::Minus => Operator::Minus,
            BinaryOperator::Multiply => Operator::Multiply,
            BinaryOperator::Divide => Operator::Divide,
            BinaryOperator::Modulus => Operator::Modulos,
            BinaryOperator::And => Operator::And,
            BinaryOperator::Or => Operator::Or,
            _ => unimplemented!(),
        };
        Ok(LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(self.sql_to_expr(left)?),
            op,
            right: Box::new(self.sql_to_expr(right)?),
        }))
    }


}

// Normalize an identifer to a lowercase string unless the identifier is quoted.
fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}

fn extract_join_keys(
    expr: &LogicalExpr,
    accum: &mut Vec<(Column, Column)>,
    accum_filter: &mut Vec<LogicalExpr>,
) {
    match expr {
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (LogicalExpr::Column(l), LogicalExpr::Column(r)) => {
                    accum.push((l.clone(), r.clone()));
                }
                _other => {
                    accum_filter.push(expr.clone());
                }
            },
            Operator::And => {
                extract_join_keys(left, accum, accum_filter);
                extract_join_keys(right, accum, accum_filter);
            }
            _other
                if matches!(**left, LogicalExpr::Column(_))
                    || matches!(**right, LogicalExpr::Column(_)) =>
            {
                accum_filter.push(expr.clone());
            }
            _other => {
                extract_join_keys(left, accum, accum_filter);
                extract_join_keys(right, accum, accum_filter);
            }
        },
        _other => {
            accum_filter.push(expr.clone());
        }
    }
}

/// 提取连接键
fn extract_possible_join_keys(expr: &LogicalExpr, accum: &mut Vec<(Column, Column)>) -> Result<()> {
    match expr {
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (LogicalExpr::Column(l), LogicalExpr::Column(r)) => {
                    accum.push((l.clone(), r.clone()));
                    Ok(())
                }
                _ => Ok(()),
            },
            Operator::And => {
                extract_possible_join_keys(left, accum)?;
                extract_possible_join_keys(right, accum)
            }
            _ => Ok(()),
        },
        _ => Ok(()),
    }
}

// 从where子句中去除连接相关内容
fn remove_join_expressions(
    expr: &LogicalExpr,
    join_columns: &HashSet<(Column, Column)>,
) -> Result<Option<LogicalExpr>> {
    match expr {
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (LogicalExpr::Column(l), LogicalExpr::Column(r)) => {
                    if join_columns.contains(&(l.clone(), r.clone()))
                        || join_columns.contains(&(r.clone(), l.clone()))
                    {
                        Ok(None)
                    } else {
                        Ok(Some(expr.clone()))
                    }
                }
                _ => Ok(Some(expr.clone())),
            },
            Operator::And => {
                let l = remove_join_expressions(left, join_columns)?;
                let r = remove_join_expressions(right, join_columns)?;
                match (l, r) {
                    (Some(ll), Some(rr)) => Ok(Some(LogicalExpr::and(ll, rr))),
                    (Some(ll), _) => Ok(Some(ll)),
                    (_, Some(rr)) => Ok(Some(rr)),
                    _ => Ok(None),
                }
            }
            _ => Ok(Some(expr.clone())),
        },
        _ => Ok(Some(expr.clone())),
    }
}
