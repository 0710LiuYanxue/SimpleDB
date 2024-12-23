use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;
use arrow::record_batch::RecordBatch;
use arrow::array::BooleanArray;

use crate::physical_plan::PhysicalPlan;
use crate::physical_plan::PhysicalPlanRef;
use crate::datasource::TableRef;
use crate::physical_plan::PhysicalExprRef;
use crate::datasource::CsvTable;
use std::sync::Arc;

#[derive(Debug)]
pub struct DeletePlan {
    source: TableRef,
    input: PhysicalPlanRef,
    conditions: PhysicalExprRef,
}

impl DeletePlan {
    pub fn create(input: PhysicalPlanRef, conditions: PhysicalExprRef, source: TableRef) -> PhysicalPlanRef {
        Arc::new(Self {input, conditions,source })
    }
    
}

// 
impl PhysicalPlan for DeletePlan{
    fn schema(&self) -> &NaiveSchema {
        self.input.schema()
    }

    // scan 方法用于从表中获取数据。
    // projection.clone() 表示是否使用列投影来选择特定的列。如果没有列投影，则扫描整个表。
    fn execute(&self) -> Result<Vec<RecordBatch>>{
        // 1. 首先，执行输入的物理计划 在这里是获取源表的所有RecordBatch
        let record_batches = self.input.execute()?;
        // 2. 遍历所有RecordBatch，并检查是否满足删除条件
        // 评估删除条件，得到符合条件的行号
        let predicate = self.conditions.evaluate(&record_batches[0])?.into_array();
        let predicate = predicate.as_any().downcast_ref::<BooleanArray>().unwrap();

        let mut rows_to_delete = vec![];

        // 找到符合删除条件的行
        for (idx, is_valid) in predicate.iter().enumerate() {
            if let Some(true) = is_valid {
                rows_to_delete.push(idx); // 记录符合条件的行号
            }
        }

        // 调用try_delete函数删除符合条件的行 这个新的表 是可以加入到原始的catalog中的
        CsvTable::try_delete(self.source.clone(), rows_to_delete)
        // let table_name = self.source.schema().fields[0].get_qualifier();
        // // 直接进行解包 适合确定其一定一会是空的情况
        // let table_name_str: &str = table_name.map(|s| s.as_str()).unwrap_or("");
        
        // // clone Arc 获取所有权的引用
        // let db_clone = Arc::clone(&self.db);
        // // 获取锁以便进行修改
        // let mut db = db_clone.lock().unwrap();
        // db.catalog.remove_table(table_name_str);
        // db.catalog.tables.insert(table_name_str.to_string(), table);
        // 3. 返回一个空的 RecordBatch，因为我们修改了原表（delete在原地操作）

        // 这里只返回一个空的 RecordBatch，因为我们修改了原表（delete在原地操作）   
        // Ok(table.scan(None)?)   这个是对原始的进行修改
    }

    // children 方法返回当前物理计划的子计划。UpdatePlan 的子计划就是它的输入计划。
    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}