use crate::logical_plan::expression::ScalarValue;
use std::any::Any;
use std::sync::Arc;

use super::{PhysicalExpr, PhysicalExprRef};
use crate::datatype::ColumnValue;
use crate::Result;
use arrow::record_batch::RecordBatch;

#[derive(Debug)]
pub struct PhysicalLiteralExpr {
    pub literal: ScalarValue,
}

impl PhysicalLiteralExpr {
    pub fn create(literal: ScalarValue) -> PhysicalExprRef {
        Arc::new(Self { literal })
    }
}

impl PhysicalExpr for PhysicalLiteralExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnValue> {
        Ok(ColumnValue::Const(self.literal.clone(), input.num_rows()))
    }
}
