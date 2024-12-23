use std::collections::HashMap;   // 存储表名（String）到表引用（TableRef）的映射，是 Catalog 结构体中表管理的核心。

// 这里指的是使用当前项目的crate，而不是外部的crate
use crate::error::ErrorCode;
use crate::logical_plan::plan::{LogicalPlan, TableScan};
use crate::logical_plan::DataFrame;
use crate::{
    datasource::{CsvConfig, CsvTable, TableRef},
    error::Result,
};

#[derive(Default, Debug)]
pub struct Catalog {
    pub tables: HashMap<String, TableRef>,
}

impl Catalog {
    // 三种表的构建 最终都需要将生成的表source插入到tables中，其中键是表名，值是表的引用。
    // 删除指定名称的表
    pub fn remove_table(&mut self, table_name: &str) -> Option<TableRef> {
        self.tables.remove(table_name)
    }
    
    /// add csv table  
    pub fn add_csv_table(
        &mut self,
        table: &str,
        csv_file: &str,      // 文件路径
        csv_conf: CsvConfig,  // 配置
    ) -> Result<()> {
        let source = 
        CsvTable::try_create(table, 
            csv_file, csv_conf)?;
        self.tables.insert(table.to_string(), source);
        Ok(())
    }


    #[allow(unused)]
    pub fn add_new_table(
        &mut self,
        table: String,
        source: TableRef,
    ) -> Result<()> {
        self.tables.insert(table, source);
        Ok(())
    }

    /// get table   根据表名获取表的引用 table_res
    pub fn get_table(&self, table: &str) -> Result<TableRef> {
        self.tables
            .get(table)
            .cloned()
            .ok_or_else(|| ErrorCode::NoSuchTable(format!("No table name: {}", table)))
    }

    #[allow(unused)]
    /// get dataframe by table name   获取数据帧以执行查询
    pub fn get_table_df(&self, table: &str) -> Result<DataFrame> {
        let source = self
            .tables
            .get(table)
            .cloned()
            .ok_or_else(|| ErrorCode::NoSuchTable(format!("No table name: {}", table)))?;
        let plan = LogicalPlan::TableScan(TableScan {
            source,
            projection: None,
        });
        Ok(DataFrame { plan })
    }
}
