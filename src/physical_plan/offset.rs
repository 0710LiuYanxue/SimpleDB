use super::{PhysicalPlan, PhysicalPlanRef};
use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;

use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PhysicalOffsetPlan {
    input: PhysicalPlanRef,
    n: usize,
}

impl PhysicalOffsetPlan {
    pub fn create(input: PhysicalPlanRef, n: usize) -> PhysicalPlanRef {
        Arc::new(Self { input, n })
    }
}

impl PhysicalPlan for PhysicalOffsetPlan {
    fn schema(&self) -> &NaiveSchema {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let batches = self.input.execute()?;
        let mut n = self.n;
        let mut ret = vec![];

        for batch in &batches {
            if n == 0 {
                ret.push(batch.clone());
                continue;
            }

            if n >= batch.num_rows() {
                n -= batch.num_rows();
                continue;
            }

            let remain = batch.num_rows() - n;
            ret.push(batch.slice(n, remain));
            n = 0;
        }
        Ok(ret)
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}
