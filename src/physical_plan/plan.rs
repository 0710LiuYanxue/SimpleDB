use std::fmt::Debug;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::{error::Result, logical_plan::schema::NaiveSchema};

// 定义一个trait特性 在其他的物理计划的具体实现中需要实现。
// PhysicalPlan 特性包括三个方法：
// schema: 获取物理计划的输出模式（即查询结果的结构）。
// execute: 执行物理计划并返回结果。
// children: 获取物理计划的子计划。
pub trait PhysicalPlan: Debug {
    fn schema(&self) -> &NaiveSchema;

    fn execute(&self) -> Result<Vec<RecordBatch>>;

    #[allow(unused)]    // 在优化中需要使用到
    fn children(&self) -> Result<Vec<PhysicalPlanRef>>;
}

pub type PhysicalPlanRef = Arc<dyn PhysicalPlan>;
