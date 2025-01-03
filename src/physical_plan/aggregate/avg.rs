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
pub struct Avg {
    sum: f64,
    cnt: u32,
    // physical column
    col_expr: ColumnExpr,
}

impl Avg {
    pub fn create(col_expr: ColumnExpr) -> Box<dyn AggregateOperator> {
        Box::new(Self {
            sum: 0.0,
            cnt: 0,
            col_expr,
        })
    }
}

macro_rules! update_match {
    ($COL: expr, $DT: ty, $SELF: expr) => {{
        let col = $COL.as_any().downcast_ref::<PrimitiveArray<$DT>>().unwrap();
        for val in col.into_iter().flatten() {
            $SELF.sum += val as f64;
            $SELF.cnt += 1;
        }
    }};
}

macro_rules! update_value {
    ($COL: expr, $DT: ty, $IDX: expr, $SELF: expr) => {{
        let col = $COL.as_any().downcast_ref::<PrimitiveArray<$DT>>().unwrap();
        if !col.is_null($IDX) {
            $SELF.sum += col.value($IDX) as f64;
            $SELF.cnt += 1;
        }
    }};
}

impl AggregateOperator for Avg {
    fn data_field(&self, schema: &NaiveSchema) -> Result<NaiveField> {
        // find by name
        if let Some(name) = &self.col_expr.name {
            let field = schema.field_with_unqualified_name(name)?;
            return Ok(NaiveField::new(
                None,
                format!("avg({})", field.name()).as_str(),
                DataType::Float64,
                false,
            ));
        }

        if let Some(idx) = &self.col_expr.idx {
            let field = schema.field(*idx);
            return Ok(NaiveField::new(
                None,
                format!("avg({})", field.name()).as_str(),
                DataType::Float64,
                false,
            ));
        }

        Err(ErrorCode::LogicalError(
            "ColumnExpr must has name or idx".to_string(),
        ))
    }

    fn update_batch(&mut self, data: &RecordBatch) -> Result<()> {
        let col = self.col_expr.evaluate(data)?.into_array();
        match col.data_type() {
            DataType::Int64 => update_match!(col, Int64Type, self),
            DataType::UInt64 => update_match!(col, UInt64Type, self),
            DataType::Float64 => update_match!(col, Float64Type, self),
            _ => {
                return Err(ErrorCode::NotSupported(format!(
                    "Avg func for {:?} is not supported",
                    col.data_type()
                )))
            }
        }

        Ok(())
    }

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

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(Some(self.sum / self.cnt as f64)))
    }

    fn clear_state(&mut self) {
        self.sum = 0.0;
        self.cnt = 0;
    }
}
