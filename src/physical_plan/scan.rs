use std::sync::Arc;

use crate::datasource::TableRef;
use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;
use arrow::record_batch::RecordBatch;

use crate::physical_plan::PhysicalPlan;
use crate::physical_plan::PhysicalPlanRef;

#[derive(Debug, Clone)]
pub struct ScanPlan {
    source: TableRef,
    projection: Option<Vec<usize>>,
}

impl ScanPlan {
    pub fn create(source: TableRef, projection: Option<Vec<usize>>) -> PhysicalPlanRef {
        Arc::new(Self { source, projection })
    }
}

impl PhysicalPlan for ScanPlan {
    fn schema(&self) -> &NaiveSchema {
        self.source.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.source.scan(self.projection.clone())
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![])
    }
}