mod binary;
mod column;
mod literal;

pub use binary::PhysicalBinaryExpr;
pub use column::ColumnExpr;
pub use literal::PhysicalLiteralExpr;

use crate::{datatype::ColumnValue, error::Result};
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

pub trait PhysicalExpr: Debug {
    fn as_any(&self) -> &dyn Any;

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnValue>;
}

pub type PhysicalExprRef = Arc<dyn PhysicalExpr>;
