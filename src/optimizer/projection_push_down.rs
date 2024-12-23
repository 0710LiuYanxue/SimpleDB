use super::OptimizerRule;
use crate::logical_plan::plan::{LogicalPlan, TableScan, };

pub struct ProjectionPushDown;

impl OptimizerRule for ProjectionPushDown {
    fn optimize(&self, plan: &LogicalPlan) -> LogicalPlan {
        match plan {
            // 如果当前计划是 ProjectionPlan，则尝试将投影下推
            LogicalPlan::Projection(projection_plan) => {
                let projection_exprs = &projection_plan.exprs;
                let input_plan = &projection_plan.input;    

                // 如果子计划是 TableScan，则可以下推投影
                if let LogicalPlan::TableScan(scan_plan) = &**input_plan {
                    // 获取 TableScan 的投影
                    let existing_projection = scan_plan.projection.clone();

                    // 合并投影表达式
                    let new_projection = Some(projection_exprs.iter().enumerate().map(|(index, _expr)| {
                        // 假设我们有一个方法可以将 LogicalExpr 转换为列索引
                        // 这里我们手动根据表达式的顺序来生成索引
                        // 例如，假设 expr 是直接可以转换为列索引的（只做简单的索引映射）
                        index
                    }).collect::<Vec<_>>());

                    // 创建新的 TableScan 计划，设置新的投影
                    let new_scan_plan = TableScan {
                        source: scan_plan.source.clone(),
                        projection: new_projection.or(existing_projection),
                    };

                    // 返回新的 TableScan 计划
                    LogicalPlan::TableScan(new_scan_plan)
                } else {
                    // 如果子计划不是 TableScan，则保持原有的 Projection 计划
                    plan.clone()
                }
            }
            _ => plan.clone(),
        }
    }
}

