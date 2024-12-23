use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;
use arrow::record_batch::RecordBatch;

use crate::physical_plan::PhysicalPlan;
use crate::physical_plan::PhysicalPlanRef;

use std::sync::Arc;


#[derive(Debug)]
pub struct CreateTablePlan {
    schema: NaiveSchema,
}

impl CreateTablePlan {
    pub fn create(schema: NaiveSchema) -> PhysicalPlanRef {
        Arc::new(Self {schema})
    }
    
}

// 
impl PhysicalPlan for CreateTablePlan{
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }

    // scan 方法用于从表中获取数据。
    // projection.clone() 表示是否使用列投影来选择特定的列。如果没有列投影，则扫描整个表。
    fn execute(&self) -> Result<Vec<RecordBatch>>{
        
        Ok(vec![])
    }

    // children 方法返回当前物理计划的子计划。UpdatePlan 的子计划就是它的输入计划。
    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![])
    }
}