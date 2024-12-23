use std::sync::Arc;

use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;
use crate::physical_plan::PhysicalPlan;
use crate::physical_plan::PhysicalPlanRef;
use crate::error::ErrorCode;
use sqlparser::ast::Expr;
use sqlparser::ast::Value;
use sqlparser::ast::SetExpr;

#[derive(Debug, Clone)]
pub struct InsertPlan {
    /// The list of expressions representing the values to be inserted
    pub source: SetExpr,  // Values for the new tuple(s)
    /// 前面的计划
    pub input: PhysicalPlanRef,
}

impl InsertPlan {
    pub fn create(source: SetExpr, input: PhysicalPlanRef) -> PhysicalPlanRef {
        Arc::new(Self {
            source,
            input,
        })
    }
    // 解析 VALUES 操作，将值转换为列数据
    fn parse_values(&self, values: Vec<Vec<Expr>>) -> Result<Vec<RecordBatch>> {
        // 假设 VALUES 是一个简单的列表，每一行数据代表一个插入元组
        let mut record_batches = Vec::new();

        for value_row in values {
            let mut columns = Vec::new();
            for (_i, value) in value_row.iter().enumerate() {
                // let column_name = &self.columns[i].value;
                let column_data = self.value_to_column_data(value)?;
                columns.push(column_data);
            }

            // 这里我们假设每行数据的列数和目标表的列数一致
            let naive_schema = self.input.schema();
            let schema = naive_schema.clone().into();
            let schema_arc: Arc<Schema> = Arc::new(schema);
            let batch = RecordBatch::try_new(schema_arc, columns)?;
            record_batches.push(batch);
        }

        Ok(record_batches)
    }

    // 将一个值转化为列数据（例如数字、字符串等）
    fn value_to_column_data(&self, expr: &Expr) -> Result<arrow::array::ArrayRef> {
        match expr {
            Expr::Value(Value::Number(num_str, _)) => {
                if num_str.contains('.') {
                    // 处理浮动类型
                    let num: f64 = num_str.parse().map_err(|e| ErrorCode::LogicalError(format!("Invalid float constant: {}", e)))?;
                    Ok(Arc::new(arrow::array::Float64Array::from(vec![num; 1])))
                } else {
                    // 处理整数类型
                    let num: i64 = num_str.parse().map_err(|e| ErrorCode::LogicalError(format!("Invalid integer constant: {}", e)))?;
                    Ok(Arc::new(arrow::array::Int64Array::from(vec![num; 1])))
                }
            }
            Expr::Value(Value::SingleQuotedString(s)) => {
                // 处理字符串
                Ok(Arc::new(arrow::array::StringArray::from(vec![s.clone(); 1])))
            }
            Expr::Value(Value::Boolean(b)) => {
                // 处理布尔值
                Ok(Arc::new(arrow::array::BooleanArray::from(vec![*b; 1])))
            }
            Expr::Value(Value::Null) => {
                // 处理 NULL
                Ok(Arc::new(arrow::array::StringArray::from(vec![None; 1])))
            }
            _ => todo!("Other value types not yet supported"),
        }
    }

    fn insert_into_table(&self, mut original_batches: Vec<RecordBatch>, new_batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        // 将新插入的批次追加到原始批次中
        original_batches.extend(new_batches);
        
        // 返回合并后的批次
        Ok(original_batches)
    }

}

// 
impl PhysicalPlan for InsertPlan {
    fn schema(&self) -> &NaiveSchema {
        self.input.schema()
    }

    // 执行插入操作
    fn execute(&self) -> Result<Vec<RecordBatch>> {
        // 解析 Values
        let values = match &self.source {
            SetExpr::Values(values) => values.clone(),  // 假设 source 是 Values 类型
            _ => return Err(ErrorCode::LogicalError("Invalid SetExpr type for Insert".to_string())),
        };

        // 将 VALUES 转换为 RecordBatch 列表
        let values_vec: Vec<Vec<Expr>> = values.0.into_iter().collect();
        let new_batches = self.parse_values(values_vec)?;
        let original_batches = self.input.execute()?;
        // 将新插入的数据添加到原始数据中
        let merged_batches = self.insert_into_table(original_batches, new_batches)?;
        // 插入到目标表
        // self.insert_into_table(record_batches.clone())?;

        // 返回插入的数据批次
        Ok(merged_batches)
    }

    // children 方法返回当前物理计划的子计划。UpdatePlan 的子计划就是它的输入计划。
    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}

