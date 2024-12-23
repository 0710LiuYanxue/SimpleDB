pub mod avg;
pub mod count;
pub mod max;
pub mod min;
pub mod sum;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use crate::error::ErrorCode;
use crate::logical_plan::schema::NaiveField;
use crate::logical_plan::{expression::ScalarValue, schema::NaiveSchema};

use super::{concat_batches, PhysicalPlan, PhysicalPlanRef};

use crate::physical_plan::PhysicalExprRef;
use crate::Result;
use arrow::array::{PrimitiveArray, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, UInt64Type};
use arrow::record_batch::RecordBatch;

#[derive(Debug)]
pub struct PhysicalAggregatePlan {
    pub group_expr: Vec<PhysicalExprRef>,    // group by 的列
    pub aggr_ops: Mutex<Vec<Box<dyn AggregateOperator>>>,  // 聚合操作集合
    pub input: PhysicalPlanRef,
    pub schema: NaiveSchema,
}

impl PhysicalAggregatePlan {
    pub fn create(
        group_expr: Vec<PhysicalExprRef>,
        aggr_ops: Vec<Box<dyn AggregateOperator>>,
        input: PhysicalPlanRef,
    ) -> PhysicalPlanRef {
        let schema = input.schema().clone();
        Arc::new(Self {
            group_expr,
            aggr_ops: Mutex::new(aggr_ops),
            input,
            schema,
        })
    }
}

// group by 聚合逻辑
macro_rules! group_by_datatype {
    ($VAL: expr, $DT: ty, $GROUP_DT: ty, $GROUP_IDXS: expr, $AGGR_OPS: expr, $SINGLE_BATCH: expr, $SCHEMA: expr, $LEN: expr) => {{
        // 从分组的列中获取的groupby值的计算数据 primitive array是一个表示基本类型的数组
        let group_val = $VAL.as_any().downcast_ref::<PrimitiveArray<$DT>>().unwrap();
        
        // 初始化分组映射 键是分组的值 值是该分组包含的行的索引列表 
        let mut group_idxs = HashMap::<$GROUP_DT, Vec<usize>>::new();

        //  遍历数据并进行分组 按其值将数据行的索引分类到不同的分组中 存在则添加，不存在则新建
        for (idx, val) in group_val.iter().enumerate() {
            if let Some(val) = val {
                if let Some(idxs) = group_idxs.get_mut(&val) {
                    idxs.push(idx);
                } else {
                    group_idxs.insert(val, vec![idx]);
                }
            }
        }

        // 对于每一个分组，遍历改组内的数据行，更新聚合操作
        // signle batch包含了所有的数据 idx是当前在同一个组的索引 根据索引 计算这个组中的全部的数据
        let mut batches = vec![];

        for group_idx in group_idxs.values() {
            for idx in group_idx {
                for i in 0..$LEN {
                    $AGGR_OPS.get_mut(i).unwrap().update(&$SINGLE_BATCH, *idx)?;
                }
            }

            let mut arrays = vec![];
            // let aggr_ops = self.aggr_ops.lock().unwrap();
            for aggr_op in $AGGR_OPS.iter() {
                let x = aggr_op.evaluate()?;
                arrays.push(x.into_array(1));
            }

            let record_batch = RecordBatch::try_new($SCHEMA.clone(), arrays)?;
            batches.push(record_batch);

            // for next group aggregate usage
            for i in 0..$LEN {
                $AGGR_OPS.get_mut(i).unwrap().clear_state();
            }
        }

        let single_batch = concat_batches(&$SCHEMA, &batches)?;
        Ok(vec![single_batch])
    }};
}

impl PhysicalPlan for PhysicalAggregatePlan {
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        // output schema
        let mut aggr_ops = self.aggr_ops.lock().unwrap();
        let len = aggr_ops.len();
        let mut fields: Vec<Field> = vec![];   // fields 用来存储输出字段的集合，字段的数量由 aggr_ops 的长度决定。
        for aggr_op in aggr_ops.iter() {
            fields.push(aggr_op.data_field(self.schema())?.into());
        }
        let schema = Arc::new(Schema::new(fields));   // 根据输出字段的确定构建输出的 schema

        // 没有Group by的聚合查询 直接计算
        if self.group_expr.is_empty() {
            let batches = self.input.execute()?;

            // 对于每个batch的数据，调用每个聚合函数的update_batch方法，更新聚合状态
            for batch in &batches {
                for i in 0..len {
                    aggr_ops.get_mut(i).unwrap().update_batch(batch)?;
                }
            }

            let mut arrays = vec![];
            for aggr_op in aggr_ops.iter() {
                let x = aggr_op.evaluate()?;    // 获取最终的聚合结果
                arrays.push(x.into_array(1));     // 实际上就是一个元组 多个列 列就是fields刚才构建的属性
            }

            // 使用计算得到的 arrays 和生成的 schema 创建一个新的 RecordBatch
            let record_batch = RecordBatch::try_new(schema, arrays)?;
            Ok(vec![record_batch])    
        } else {   // 存在Group by的聚合查询
            // such as `select sum(id) from t1 group by id % 3, age % 2` 进一步扩展
            let batches = self.input.execute()?;
            // 将多个batch合并在一起 因为groupby需要遍历整个数据集
            let single_batch = concat_batches(&self.input.schema().clone().into(), &batches)?;

            // 提取groupby的第一个表达式
            let group_by_expr = &self.group_expr[0];

            let val = group_by_expr.evaluate(&single_batch)?.into_array();
            // 根据分组值 调用 group_by_datatype! 宏处理
            // 64位有符号整数 64位无符号整数 可变长度字符串
            match val.data_type() {
                DataType::Int64 => group_by_datatype!(
                    val,
                    Int64Type,
                    i64,
                    group_idxs,
                    aggr_ops,
                    single_batch,
                    schema,
                    len
                ),
                DataType::UInt64 => group_by_datatype!(
                    val,
                    UInt64Type,
                    u64,
                    group_idxs,
                    aggr_ops,
                    single_batch,
                    schema,
                    len
                ),
                DataType::Utf8 => {
                    let group_val = val.as_any().downcast_ref::<StringArray>().unwrap();
                
                    let mut group_idxs = HashMap::<String, Vec<usize>>::new();

                    // split into different groups
                    for (idx, val) in group_val.iter().enumerate() {
                        if let Some(val) = val {
                            if let Some(idxs) = group_idxs.get_mut(val) {
                                idxs.push(idx);
                            } else {
                                group_idxs.insert(val.to_string(), vec![idx]);
                            }
                        }
                    }

                    // for each group, calculate aggregating value  
                    // 对于每一个分组，遍历改组内的数据行，更新聚合操作
                    let mut batches = vec![];

                    for group_idx in group_idxs.values() {
                        for idx in group_idx {
                            for i in 0..len {
                                aggr_ops.get_mut(i).unwrap().update(&single_batch, *idx)?;
                            }
                        }

                        let mut arrays = vec![];
                        // let aggr_ops = self.aggr_ops.lock().unwrap();
                        for aggr_op in aggr_ops.iter() {
                            let x = aggr_op.evaluate()?;
                            arrays.push(x.into_array(1));
                        }

                        let record_batch = RecordBatch::try_new(schema.clone(), arrays)?;
                        batches.push(record_batch);

                        // for next group aggregate usage
                        for i in 0..len {
                            aggr_ops.get_mut(i).unwrap().clear_state();
                        }
                    }

                    let single_batch = concat_batches(&schema, &batches)?;
                    Ok(vec![single_batch])
                }
                _ => Err(ErrorCode::NotSupported(
                    "group by only support by `Int64`, `UInt64`, `String`".to_string(),
                )),
            }
        }
    }
}

pub trait AggregateOperator: Debug {
    fn data_field(&self, schema: &NaiveSchema) -> Result<NaiveField>;

    fn update_batch(&mut self, data: &RecordBatch) -> Result<()>;

    fn update(&mut self, data: &RecordBatch, idx: usize) -> Result<()>;

    fn evaluate(&self) -> Result<ScalarValue>;

    fn clear_state(&mut self);
}
