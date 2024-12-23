
// 利用外部的crate，这并不是rust标准库的一部分
// 需要在Cargo.toml中进行声明它是项目的依赖 sqlparser = "0.9.0"
use sqlparser::{   
    ast::Statement,            // 解析后的 SQL 抽象语法树（AST）的主要结构
    dialect::GenericDialect,   // SQL通用方言，支持标准SQL语法 需要解析特定数据库的 SQL，可以替换为对应的方言（如 PostgreSqlDialect）
    parser::{Parser, ParserError},
    tokenizer::Tokenizer,   // 词法分析器
};

/// SQL Parser
pub struct SQLParser;   // 空结构体，没有内部字段，仅作为命名空间来定义相关的方法

impl SQLParser {
    // 成功时返回 SQL AST（statement） 失败时返回ParserError并描述遇到的问题
    pub fn parse(sql: &str) -> Result<Statement, ParserError> {
        let dialect = GenericDialect {}; 
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize()?; //  SQL 字符串分解为标记（tokens） ? 操作符会在词法分析失败时提前返回错误。
        let mut parser = Parser::new(tokens, &dialect);
        parser.parse_statement()     // 解析结果是AST 类型是Statement::Query
    }
}
