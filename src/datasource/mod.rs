mod csv; 

use std::fmt::Debug;
use std::sync::Arc;

use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;
use arrow::record_batch::RecordBatch;

// 类型别名，表示一个Arc（原子引用计数智能指针）持有的 TableSource trait 对象。
// 动态大小，可以指向任何实现了TableSource trait的对象，CsvTable、MemTable 或 EmptyTable 等
pub type TableRef = Arc<dyn TableSource>;  

pub trait TableSource: Debug {     // 类似于一个接口，定义了一组方法的签名，但是不包含具体的实现。
    fn schema(&self) -> &NaiveSchema;

    /// for scan
    fn scan(&self, projection: Option<Vec<usize>>) -> Result<Vec<RecordBatch>>;

    fn source_name(&self) -> String;
}

pub use csv::CsvConfig;      // 将子模块的特定项公开到父模块的外部。
pub use csv::CsvTable;