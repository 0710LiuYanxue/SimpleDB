# 利用Rust实现一个支持SQL接口的查询引擎


## Architecture

![query_engine](./doc/Rust.drawio.png)

## how to use 


```rust
impl NaiveDB {
    pub fn run_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // 1. sql -> statement
        let statement = SQLParser::parse(sql)?;
        // 2. statement -> logical plan
        let sql_planner = SQLPlanner::new(&self.catalog);
        let logical_plan = sql_planner.statement_to_plan(statement)?;
        // 3. optimize
        let optimizer = Optimizer::default();
        let logical_plan = optimizer.optimize(logical_plan);
        // 4. logical plan -> physical plan
        let physical_plan = QueryPlanner::create_physical_plan(&logical_plan)?;
        // 5. execute
        physical_plan.execute()
    }
}
```


## Future Work
1. SQL功能扩展
    - [ ] **更完整的SQL支持**：支持更多的SQL功能，如HAVING、ORDER BY、窗口函数、子查询等；
    - [ ] **复杂查询**：支持更多JOIN操作（如LEFT JOIN、RIGHT JOIN）、集合操作（UNION、INTERSECT、EXCEPT）等。
2. 查询优化
    - [ ] **实现更多的优化规则**：谓词下推、谓词合并、冗余谓词消除、常量折叠、连接重排序、提前聚合等；
    - [ ] **连接算法**：支持更多的连接，例如嵌套索引连接，根据数据分布，表的大小和索引情况等选择最合适的连接算法。
3. 持久化与存储引擎
    - [ ] **实现持久化存储引擎**：选择合适的数据存储结构，将数据存储到磁盘上，并支持高效的读写；
    - [ ] **缓存管理和内存映射**：将磁盘上的数据文件映射到内存中，直接通过内存来访问磁盘数据，减少读取磁盘的时间。
4. 事务支持
    - [ ] **事务日志和恢复机制**：确保事务的原子性、持久性和系统崩溃后的快速恢复，同时减少日志存储开销；
    - [ ] **并发控制**：利用MVCC和不同的事务隔离级别实现并发控制，提高事务吞吐量，同时确保数据一致性和事务隔离性。
## Conclusion
在本次的课程项目中，我通过Rust实现了一个简单的SQL查询引擎，并在此过程中对Rust的基本概念有了更深入的理解，学习了Rust的一些基本语法、数据结构和语言的独特特性。我深刻意识到Rust的内存管理模型、性能优化能力和并发处理的强大优势。这些特性在构建高性能和可靠的查询引擎中至关重要，它们允许高效的内存使用、速度和安全的并发操作。