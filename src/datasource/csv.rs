use std::env;
use std::fs::File;
use std::iter::Iterator;
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;

use arrow::csv;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::TableSource;
use crate::datasource::TableRef;
use arrow::datatypes::DataType;
use arrow::array::{Array, BooleanArray, UInt64Array, Int64Array, Float64Array, StringArray};
use arrow::array::StringBuilder;
use arrow::array::BooleanBuilder;
use arrow::array::UInt64Builder;
use arrow::array::Int64Builder;
use arrow::array::Float64Builder;

pub struct CsvConfig {
    pub has_header: bool,
    pub delimiter: u8,     // 字段之间的分隔符，默认是‘，’
    pub max_read_records: Option<usize>,
    pub batch_size: usize,
    pub file_projection: Option<Vec<usize>>,
    pub datetime_format: Option<String>,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            max_read_records: Some(3),
            batch_size: 1_000_000,
            file_projection: None,
            datetime_format: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct 
CsvTable {
    pub schema: NaiveSchema,     // 表的模式 元数据 结构信息
    pub batches: Vec<RecordBatch>,   // 数据
}

impl CsvTable {
    #[allow(unused, clippy::iter_next_loop)]
    pub fn try_create(table_name: &str, filename: &str, csv_config: CsvConfig) -> Result<TableRef> {
        // 1. 读取csv文件，获取原始schema
        let orig_schema = Self::infer_schema_from_csv(filename, &csv_config)?;
        let mut schema = NaiveSchema::from_unqualified(&orig_schema);
        schema.fields[0].set_qualifier(Some(table_name.to_string()));

        // 2. 读取csv文件，获取原始数据，构建 RecordBatch
        let mut file = File::open(env::current_dir()?.join(Path::new(filename)))?;
        // 3. 使用 Arrow 提供的工具函数 read_csv，读取 CSV 文件，构建 RecordBatch。
        let mut reader = csv::Reader::new(
            file,
            Arc::new(orig_schema),
            csv_config.has_header,
            Some(csv_config.delimiter),
            csv_config.batch_size,
            None,
            csv_config.file_projection.clone(),
            csv_config.datetime_format,
        );
        // 4. 逐批读取数据
        let mut batches = vec![];

        // 5. 构造CsvTable并返回
        for record in reader.by_ref() {
            batches.push(record?);
        }
        Ok(Arc::new(Self {schema, batches }))
    }

    // 删除指定位置的列
    pub fn try_delete(table: TableRef, row_indices_to_delete: Vec<usize>) -> Result<Vec<RecordBatch>> {
        // 获取原始的表格模式
        let schema = table.schema().clone();
        let mut batches = table.scan(None)?;

        // 遍历每个 RecordBatch 进行删除
        for batch in &mut batches {
            let mut columns = vec![];
            for col in batch.columns() {
                let dt = col.data_type();
                let column: Arc<dyn Array> = match dt {
                    DataType::Boolean => {
                        // 删除指定行的布尔列
                        let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                        let mut builder = BooleanBuilder::new(array.len() - row_indices_to_delete.len());
                        for (i, valid) in array.iter().enumerate() {
                            if !row_indices_to_delete.contains(&i) {
                                builder.append_option(valid)?;
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::UInt64 => {
                        // 删除指定行的无符号 64 位整数列
                        let array = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                        let mut builder = UInt64Builder::new(array.len() - row_indices_to_delete.len());
                        for (i, val) in array.iter().enumerate() {
                            if !row_indices_to_delete.contains(&i) {
                                builder.append_option(val)?;
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::Int64 => {
                        // 删除指定行的有符号 64 位整数列
                        let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        let mut builder = Int64Builder::new(array.len() - row_indices_to_delete.len());
                        for (i, val) in array.iter().enumerate() {
                            if !row_indices_to_delete.contains(&i) {
                                builder.append_option(val)?;
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::Float64 => {
                        // 删除指定行的浮动 64 位列
                        let array = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        let mut builder = Float64Builder::new(array.len() - row_indices_to_delete.len());
                        for (i, val) in array.iter().enumerate() {
                            if !row_indices_to_delete.contains(&i) {
                                builder.append_option(val)?;
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::Utf8 => {
                        // 删除指定行的字符串列
                        let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                        let mut builder = StringBuilder::new(array.len() - row_indices_to_delete.len());
                        for (i, val) in array.iter().enumerate() {
                            if !row_indices_to_delete.contains(&i) {
                                builder.append_option(val)?;
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    _ => unimplemented!(),
                };
                columns.push(column);
            }
            // 更新 RecordBatch，去除已删除的行
            *batch = RecordBatch::try_new(Arc::new(schema.clone().into()), columns)?;
        }
        Ok(batches)

    }
    

    
    fn infer_schema_from_csv(filename: &str, csv_config: &CsvConfig) -> Result<Schema> {
        // 1. 打开文件，读取第一行数据，获取原始schema
        // 2. 使用 Arrow 提供的工具函数 infer_reader_schema，分析 CSV 文件的前几行数据来确定模式。
        let mut file = File::open(env::current_dir()?.join(Path::new(filename)))?;
        let (schema, _) = arrow::csv::reader::infer_reader_schema(
            &mut file,
            csv_config.delimiter,
            csv_config.max_read_records,
            csv_config.has_header,
        )?;
        Ok(schema)
    }
}

impl TableSource for CsvTable {
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }
    // 实现其对应的扫描操作
    fn scan(&self, _projection: Option<Vec<usize>>) -> 
        Result<Vec<RecordBatch>> {
        Ok(self.batches.clone())
    }
    fn source_name(&self) -> String {
        "CsvTable".into()
    }
}
