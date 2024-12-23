use std::sync::Arc;

use super::{PhysicalExprRef, PhysicalPlan, PhysicalPlanRef};
use crate::logical_plan::schema::NaiveSchema;
use crate::Result;
use arrow::array::{
    Float64Array, Float64Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
    UInt64Array, UInt64Builder,
};
use arrow::record_batch::RecordBatch;
use arrow::{
    array::{Array, BooleanArray, BooleanBuilder},
    datatypes::DataType,
};

#[derive(Debug)]
pub struct SelectionPlan {
    input: PhysicalPlanRef,
    expr: PhysicalExprRef,
}

impl SelectionPlan {
    pub fn create(input: PhysicalPlanRef, expr: PhysicalExprRef) -> PhysicalPlanRef {
        Arc::new(Self { input, expr })
    }
}

macro_rules! build_array_by_predicate {
    ($COLUMN: ident, $PREDICATE: expr, $ARRAY_TYPE: ty, $ARRAY_BUILDER: ty) => {{
        let array = $COLUMN.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let mut builder = <$ARRAY_BUILDER>::new(array.len());
        let iter = $PREDICATE.iter().zip(array.iter());
        for (valid, val) in iter {
            match valid {
                Some(valid) => {
                    if valid {
                        builder.append_option(val)?;
                    }
                }
                None => builder.append_option(None)?,
            }
        }
        Arc::new(builder.finish())
    }};
}

impl PhysicalPlan for SelectionPlan {
    fn schema(&self) -> &NaiveSchema {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let input = self.input.execute()?;
        let predicate = self.expr.evaluate(&input[0])?.into_array();
        let predicate = predicate.as_any().downcast_ref::<BooleanArray>().unwrap();

        let mut batches = vec![];

        for batch in &input {
            let mut columns = vec![];
            for col in batch.columns() {
                let dt = col.data_type();
                let column: Arc<dyn Array> = match dt {
                    DataType::Boolean => {
                        build_array_by_predicate!(col, predicate, BooleanArray, BooleanBuilder)
                    }
                    DataType::UInt64 => {
                        build_array_by_predicate!(col, predicate, UInt64Array, UInt64Builder)
                    }
                    DataType::Int64 => {
                        build_array_by_predicate!(col, predicate, Int64Array, Int64Builder)
                    }
                    DataType::Float64 => {
                        build_array_by_predicate!(col, predicate, Float64Array, Float64Builder)
                    }
                    DataType::Utf8 => {
                        let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                        let mut builder = StringBuilder::new(array.len());
                        let iter = predicate.iter().zip(array.iter());
                        for (valid, val) in iter {
                            match valid {
                                Some(valid) => {    // 如果 valid 为 Some(true)，即该行满足选择条件
                                    if valid {
                                        builder.append_option(val)?;
                                    }
                                }    // 如果 valid 为 Some(false)，即该行不满足选择条件，则不需要加入到数组中
                                None => builder.append_option(None::<&str>)?,
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    _ => unimplemented!(),
                };
                columns.push(column);
            }
            let record_batch =      // 生成过滤后的列数组
                RecordBatch::try_new(Arc::new(self.schema().clone().into()), columns)?;
            batches.push(record_batch);
        }
        Ok(batches)
    }

    // children 方法返回当前物理计划的子计划。UpdatePlan 的子计划就是它的输入计划。
    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}