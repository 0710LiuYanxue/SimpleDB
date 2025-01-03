use simple_db::print_result;
use simple_db::CsvConfig;
use simple_db::SimpleDB;
use simple_db::Result;
use std::sync::{Arc, Mutex};
use arrow::record_batch::RecordBatch;
use std::io::{self, Write};  // 引入标准输入输出模块


fn run_sql_on_db(db_arc: Arc<Mutex<SimpleDB>>, sql: &str) -> Result<Vec<RecordBatch>> {
    let mut db = db_arc.lock().unwrap(); // 获取锁，修改 db
    db.run_sql(sql)
}

fn main() -> Result<()> {
    // 创建数据库
    let mut db = SimpleDB::default();
    println!("Welcome to Snow's SimpleDB!");
    // 初始化一个内存表t1和三个内存表employee、rank和department以及person、knows表
    db.create_csv_table("t1", "data/test_data.csv", CsvConfig::default())?;
    db.create_csv_table("person", "data/person.csv", CsvConfig::default())?;
    db.create_csv_table("knows", "data/knows.csv", CsvConfig::default())?;
    db.create_csv_table("employee", "data/employee.csv", CsvConfig::default())?;
    db.create_csv_table("rank", "data/rank.csv", CsvConfig::default())?;
    db.create_csv_table("department", "data/department.csv", CsvConfig::default())?;

    // 创建数据库的引用
    let db_arc = Arc::new(Mutex::new(db));
    // 进入一个命令行交互模式
    loop {
        // 提示用户输入 SQL 查询
        print!("Enter SQL query (or 'exit' to quit): ");
        io::stdout().flush()?; // 确保输出立即显示

        // 读取用户输入的查询
        let mut sql = String::new();
        io::stdin().read_line(&mut sql)?;

        // 去除末尾的换行符
        let sql = sql.trim();

        // 如果用户输入 "exit"，则退出程序
        if sql.to_lowercase() == "exit" {
            println!("Exiting the database system.");
            break;
        }

        // 执行查询并输出结果
        match run_sql_on_db(db_arc.clone(), sql) {
            Ok(result) => {
                print_result(&result)?;
            }
            Err(e) => {
                println!("Error executing query '{}': {:?}", sql, e);
            }
        }
    }

    Ok(())
}
