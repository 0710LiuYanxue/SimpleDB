use arrow::array::Array;
use arrow::array::PrimitiveArray;
use arrow::datatypes::DataType;

use arrow::datatypes::Float64Type;
use arrow::datatypes::Int64Type;
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;

use super::AggregateOperator;
use crate::error::ErrorCode;
use crate::logical_plan::expression::ScalarValue;
use crate::logical_plan::schema::NaiveField;
use crate::logical_plan::schema::NaiveSchema;
use crate::physical_plan::ColumnExpr;
use crate::physical_plan::PhysicalExpr;
use crate::Result;

#[derive(Debug, Clone)]
pub struct Sum {
    sum: f64,      // 初始值为0
    // physical column 
    col_expr: ColumnExpr,
}

impl Sum {
    pub fn create(col_expr: ColumnExpr) -> Box<dyn AggregateOperator> {
        Box::new(Self { sum: 0.0, col_expr })
    }
}

macro_rules! update_match {
    ($COL: expr, $DT: ty, $SELF: expr) => {{
        // 将 col（列）转换为 PrimitiveArray 类型，然后遍历列中的值并累加到 self.sum。
        let col = $COL.as_any().downcast_ref::<PrimitiveArray<$DT>>().unwrap(); 
        for val in col.into_iter().flatten() {   // flatten() 是用来过滤掉 null 值，仅对非空数据进行累加
            $SELF.sum += val as f64;
        }
    }};
}

// 针对逐行更新操作，给定索引 idx，更新 self.sum 值
macro_rules! update_value {
    ($COL: expr, $DT: ty, $IDX: expr, $SELF: expr) => {{
        let col = $COL.as_any().downcast_ref::<PrimitiveArray<$DT>>().unwrap();
        if !col.is_null($IDX) {
            $SELF.sum += col.value($IDX) as f64;
        }
    }};
}

impl AggregateOperator for Sum {
    // 根据列名或索引从模式（NaiveSchema）中查找对应的字段，并生成一个新字段，类型为 Float64，表示求和结果。
    fn data_field(&self, schema: &NaiveSchema) -> Result<NaiveField> {
        // find by name
        if let Some(name) = &self.col_expr.name {
            let field = schema.field_with_unqualified_name(name)?;
            return Ok(NaiveField::new(
                None,
                format!("sum({})", field.name()).as_str(),
                DataType::Float64,
                false,
            ));
        }
        // find by index
        if let Some(idx) = &self.col_expr.idx {
            let field = schema.field(*idx);
            return Ok(NaiveField::new(
                None,
                format!("sum({})", field.name()).as_str(),
                DataType::Float64,
                false,
            ));
        }

        Err(ErrorCode::LogicalError(
            "ColumnExpr must has name or idx".to_string(),
        ))
    }

    // 通过 col_expr 计算出要聚合的列，
    // 然后根据列的数据类型（如 Int64, UInt64, Float64）选择合适的宏（update_match）来更新总和。
    fn update_batch(&mut self, data: &RecordBatch) -> Result<()> {
        let col = self.col_expr.evaluate(data)?.into_array();
        match col.data_type() {
            DataType::Int64 => update_match!(col, Int64Type, self),
            DataType::UInt64 => update_match!(col, UInt64Type, self),
            DataType::Float64 => update_match!(col, Float64Type, self),
            _ => {
                return Err(ErrorCode::NotSupported(format!(
                    "Sum func for {:?} is not supported",
                    col.data_type()
                )))
            }
        }

        Ok(())
    }

    // update 方法是针对逐行数据更新的，它处理单个数据行的更新。
    // 根据数据类型，调用相应的 update_value 宏，通过索引 idx 获取该行的列值并更新总和。
    fn update(&mut self, data: &RecordBatch, idx: usize) -> Result<()> {
        let col = self.col_expr.evaluate(data)?.into_array();
        match col.data_type() {
            DataType::Int64 => update_value!(col, Int64Type, idx, self),
            DataType::UInt64 => update_value!(col, UInt64Type, idx, self),
            DataType::Float64 => update_value!(col, Float64Type, idx, self),
            _ => unimplemented!(),
        }
        Ok(())
    }

    // evaluate 方法返回当前聚合操作的结果（即 sum 字段的值）。
    // 它将 sum 转换为 ScalarValue::Float64 类型返回，表示聚合结果。
    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(Some(self.sum)))
    }

    // clear_state 方法将 sum 重置为 0.0，清除当前的聚合状态，
    // 通常在处理下一批数据时会调用该方法。
    fn clear_state(&mut self) {
        self.sum = 0.0;
    }
}
