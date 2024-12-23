use std::iter::Iterator;
use std::sync::Arc;

use super::plan::PhysicalPlan;
use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;
use crate::physical_plan::PhysicalExprRef;
use crate::physical_plan::PhysicalPlanRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
#[derive(Debug, Clone)]
pub struct ProjectionPlan {
    input: PhysicalPlanRef,
    schema: NaiveSchema,
    expr: Vec<PhysicalExprRef>,
}

impl ProjectionPlan {
    pub fn create(
        input: PhysicalPlanRef,
        schema: NaiveSchema,
        expr: Vec<PhysicalExprRef>,
    ) -> PhysicalPlanRef {
        Arc::new(Self {
            input,
            schema,
            expr,
        })
    }
}

impl PhysicalPlan for ProjectionPlan {
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let input = self.input.execute()?;

        // when aggragating, we just output what input does
        if self.schema.fields().is_empty() {
            Ok(input)
        } else {
            let batches = input
                .iter()
                .map(|batch| {
                    let columns = self
                        .expr
                        .iter()
                        // TODO(veeupup): remove unwrap
                        .map(|expr| expr.evaluate(batch).unwrap())
                        .collect::<Vec<_>>();
                    let columns = columns
                        .iter()
                        .map(|column| column.clone().into_array())
                        .collect::<Vec<_>>();
                    // TODO(veeupup): remove unwrap
                    // let projection_schema = self.schema.into();
                    RecordBatch::try_new(SchemaRef::from(self.schema.clone()), columns).unwrap()
                })
                .collect::<Vec<_>>();
            Ok(batches)
        }
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}
