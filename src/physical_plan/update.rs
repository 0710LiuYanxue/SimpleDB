use std::sync::Arc;
use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;
use arrow::record_batch::RecordBatch;
use crate::physical_plan::PhysicalPlan;
use crate::physical_plan::PhysicalPlanRef;
use crate::physical_plan::PhysicalExprRef;
use crate::error::ErrorCode;
use sqlparser::ast::Assignment;
use sqlparser::ast::Expr;
use sqlparser::ast::Value;
use arrow::array::{BooleanArray, StringArray, Int64Array, Float64Array};
use arrow::array::ArrayRef;
use arrow::array::{StringBuilder, BooleanBuilder, Int64Builder, Float64Builder};
use arrow::array::Array;

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    input: PhysicalPlanRef,
    conditions: PhysicalExprRef,
    assignments: Vec<Assignment>, // 赋值操作，即更新的列和值
}

impl UpdatePlan {
    pub fn create(input: PhysicalPlanRef, conditions: PhysicalExprRef, assignments: Vec<Assignment>) -> PhysicalPlanRef {
        Arc::new(Self { input, conditions, assignments })
    }

    fn apply_assignments(&self, batch: RecordBatch, rows_to_update: &[usize]) -> Result<RecordBatch> {
        let mut updated_columns = Vec::new();

        // 遍历每个列，处理赋值操作
        for (i, column) in batch.columns().iter().enumerate() {
            let mut updated_column = column.clone();

            // 遍历每个赋值操作
            for assignment in &self.assignments {
                // 如果列名匹配，更新该列
                if &assignment.id.value == batch.schema().field(i).name() {
                    // 使用 update_column_with_value 方法应用赋值操作
                    updated_column = self.update_column_with_value(&updated_column, &assignment.value, rows_to_update)?;
                }
            }

            updated_columns.push(updated_column);
        }

        // 创建更新后的 RecordBatch
        let updated_batch = RecordBatch::try_new(batch.schema(), updated_columns)?;
        Ok(updated_batch)
    }
    fn update_column_with_value(
        &self,
        column: &ArrayRef,
        value: &Expr,
        rows_to_update: &[usize],
    ) -> Result<ArrayRef> {
        match value {
            Expr::Value(val) => {
                match val {
                    Value::Number(num_str, _) => {
                        if num_str.contains('.') {
                            // 处理浮动类型
                            let num: f64 = num_str.parse().map_err(|e| {
                                ErrorCode::LogicalError(format!("Invalid float constant: {}", e))
                            })?;
    
                            // 获取原始列数据
                            let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                            let mut builder = Float64Builder::new(array.len());
    
                            // 遍历列，将未更新的行保持原值，符合条件的行更新为新的值
                            for (i, val) in array.iter().enumerate() {
                                if rows_to_update.contains(&i) {
                                    builder.append_value(num)?;
                                } else {
                                    builder.append_option(val)?;
                                }
                            }
    
                            Ok(Arc::new(builder.finish()))
                        } else {
                            // 处理整数类型
                            let num: i64 = num_str.parse().map_err(|e| {
                                ErrorCode::LogicalError(format!("Invalid integer constant: {}", e))
                            })?;
    
                            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                            let mut builder = Int64Builder::new(array.len());
    
                            // 遍历列，将未更新的行保持原值，符合条件的行更新为新的值
                            for (i, val) in array.iter().enumerate() {
                                if rows_to_update.contains(&i) {
                                    builder.append_value(num)?;
                                } else {
                                    builder.append_option(val)?;
                                }
                            }
    
                            Ok(Arc::new(builder.finish()))
                        }
                    }
                    Value::SingleQuotedString(s) => {
                        // 处理字符串类型
                        let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                        let mut builder = StringBuilder::new(array.len());
    
                        // 遍历列，将未更新的行保持原值，符合条件的行更新为新的值
                        for (i, val) in array.iter().enumerate() {
                            if rows_to_update.contains(&i) {
                                builder.append_value(s.clone())?;
                            } else {
                                builder.append_option(val)?;
                            }
                        }
    
                        Ok(Arc::new(builder.finish()))
                    }
                    Value::Boolean(b) => {
                        // 处理布尔类型
                        let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                        let mut builder = BooleanBuilder::new(array.len());
    
                        // 遍历列，将未更新的行保持原值，符合条件的行更新为新的值
                        for (i, val) in array.iter().enumerate() {
                            if rows_to_update.contains(&i) {
                                builder.append_value(*b)?;
                            } else {
                                builder.append_option(val)?;
                            }
                        }
    
                        Ok(Arc::new(builder.finish()))
                    }
                    Value::Null => {
                        // 处理 Null 值类型
                        let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                        let mut builder = StringBuilder::new(array.len());
    
                        // 遍历列，将未更新的行保持原值，符合条件的行更新为 Null
                        for (i, val) in array.iter().enumerate() {
                            if rows_to_update.contains(&i) {
                                builder.append_null()?;
                            } else {
                                builder.append_option(val)?;
                            }
                        }
    
                        Ok(Arc::new(builder.finish()))
                    }
                    _ => {
                        todo!("Handle other value types");
                    }
                }
            }
            _ => {
                todo!("Handle non-value expressions");
            }
        }
    }
    
}

impl PhysicalPlan for UpdatePlan {
    fn schema(&self) -> &NaiveSchema {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        // 1. 首先，执行输入的物理计划（此时是扫描数据）
        let record_batches = self.input.execute()?;

        // 2. 遍历输入的记录，并根据更新的条件修改记录
        let mut updated_batches = Vec::new();

        // 3. 对每个 RecordBatch 进行条件评估，得到符合条件的行号
        for batch in &record_batches {
            // 评估更新条件，得到符合条件的行号
            let predicate = self.conditions.evaluate(batch)?.into_array();
            let predicate = predicate.as_any().downcast_ref::<BooleanArray>().unwrap();

            let mut rows_to_update = vec![];

            // 找到符合更新条件的行
            for (idx, is_valid) in predicate.iter().enumerate() {
                if let Some(true) = is_valid {
                    rows_to_update.push(idx); // 记录符合条件的行号
                }
            }

            // 4. 对符合条件的记录批次执行更新操作
            let updated_batch = self.apply_assignments(batch.clone(), &rows_to_update)?;
            updated_batches.push(updated_batch);
        }

        // 5. 返回更新后的记录批次
        Ok(updated_batches)
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}

