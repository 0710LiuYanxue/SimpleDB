use arrow::record_batch::RecordBatch;

use crate::catalog::Catalog;
use crate::datasource::CsvConfig;
use crate::error::Result;

use crate::optimizer::Optimizer;
use crate::planner::QueryPlanner;
use crate::sql::parser::SQLParser;
use crate::sql::planner::SQLPlanner;
use std::sync::Arc;
use sqlparser::ast::{ObjectName, Statement};
use crate::datasource::CsvTable;

#[derive(Default, Debug)]    // 自动生成一个默认实现，当调用 NaiveDB::default() 时，会创建一个默认的 NaiveDB 实例，其中 catalog 也会使用其默认值。
pub struct SimpleDB {   // 表示数据库的目录，用于存储表的元信息（如表名、字段、存储位置等）。Catalog 是一个数据结构，具体实现可能包含各种管理表和模式的功能。
    pub catalog: Catalog,
}

impl SimpleDB {
    // 执行一个sql语句 返回结果/错误 这里来回移动所有权 会造成错误
    pub fn run_sql(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        // 1. sql -> statement
        let statement1 = SQLParser::parse(sql)?;   // ? 操作符会在解析失败时提前返回错误，表示遇到没定义的语句。
        // 2. statement -> logical plan
        let sql_planner = SQLPlanner::new(&self.catalog); // 创建一个SQL查询计划，使用数据库的catalog来检查表和列的元数据。
        let logical_plan = sql_planner.statement_to_plan(statement1)?;  // ? 表示statement无法解析成计划，在执行update的时候出现这个问题，因为没定义
        // println!("{:?}", logical_plan);    // 打印出逻辑计划
        // 3. optimize
        let optimizer = Optimizer::default();
        let logical_plan = optimizer.optimize(logical_plan); 
        // 4. logical plan -> physical plan
        let physical_plan = QueryPlanner::create_physical_plan(&logical_plan)?;
        // 5. execute
        let new_table = physical_plan.execute();

        // 对于除了select以外的操作，涉及到表的修改，需要进行额外的处理
        let statement2 = SQLParser::parse(sql)?;
        let new_table2 = physical_plan.execute();
        match statement2 {      // match匹配语句
            Statement::Query(_query) => {      // 明确的匹配模式
            }
            Statement::CreateTable{or_replace:_,temporary:_, external:_, if_not_exists:_, name,columns:_,constraints:_, hive_distribution:_, hive_formats:_, table_properties:_, with_options:_, file_format:_, location:_, query:_, without_rowid:_, like:_} => {
                let table_name = self.name_convert(name);
                let schema = physical_plan.schema();
                let batches = Vec::<RecordBatch>::new();
                let table_csv = CsvTable{schema: schema.clone(), batches};
                let source = Arc::new(table_csv);
                let _ = self.catalog.add_new_table(table_name, source);
            }
            Statement::Drop{object_type:_, if_exists:_, names, cascade:_, purge:_} => {   
                for name in names {
                    let table_name = self.name_convert(name);
                    self.catalog.remove_table(&table_name);
                }
            }
            Statement::Update{table_name, assignments:_, selection:_ } => {   
                let old_table = self.name_convert(table_name);
                let table_ref = self.catalog.get_table(old_table.as_str())?;

                let schema = table_ref.schema();
                let batches = new_table?;
                let table_csv = CsvTable{schema: schema.clone(), batches};
                let source = Arc::new(table_csv);
                // self.catalog.                   
                self.catalog.remove_table(&old_table);
                let _ = self.catalog.add_new_table(old_table, source);
                
            }
            Statement::Insert{or:_, table_name, columns:_, overwrite:_, source:_, partitioned:_, after_columns:_, table:_} => {
                let old_table = self.name_convert(table_name);
                let table_ref = self.catalog.get_table(old_table.as_str())?;

                let schema = table_ref.schema();
                let batches = new_table?;
                let table_csv = CsvTable{schema: schema.clone(), batches};
                let source = Arc::new(table_csv);
                // self.catalog.                   
                self.catalog.remove_table(&old_table);
                let _ = self.catalog.add_new_table(old_table, source);
            }
            Statement::Delete{table_name, selection: _} => {
                let old_table = self.name_convert(table_name);
                let table_ref = self.catalog.get_table(old_table.as_str())?;

                let schema = table_ref.schema();
                let batches = new_table?;
                let table_csv = CsvTable{schema: schema.clone(), batches};
                let source = Arc::new(table_csv);
                // self.catalog.                   
                self.catalog.remove_table(&old_table);
                let _ = self.catalog.add_new_table(old_table, source);
            }

            _ => unimplemented!(),    // 通配符匹配模式，最初用来捕获所有不属于 Statement::Query的statement值 即如果不是Select语句调用这个位置
        }
        // 作为返回值 所以需要重新再生成一个
        
        
        new_table2     // 最后的返回值 对于select一类的操作是有意义的
    }

    pub fn name_convert(&mut self, table_name: ObjectName) -> String {
        table_name
                .0
                .iter()  // 遍历 Vec<Ident>
                .map(|ident| ident.value.clone())  // 获取每个 Ident 的 value 字段
                .collect::<Vec<String>>()  // 将所有字符串收集到一个 Vec 中
                .join(".")
    }

    // 实现将CSV文件注册为数据库中的表 
    pub fn create_csv_table(
        &mut self,
        table: &str,
        csv_file: &str,      
        csv_conf: CsvConfig, 
    ) -> Result<()> {
        self.catalog.add_csv_table(table, 
            csv_file, csv_conf)
    }

    // 实现修改指定的CSV表 传入的参数是
    // pub fn update_csv_table
    // 插入一个新的元组到CSV表中
    // pub fn insert_csv_table
    // pub fn delete_csv_table

}
