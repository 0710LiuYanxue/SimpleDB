use arrow::error::ArrowError;
use sqlparser::parser::ParserError;
use std::io;

pub type Result<T> = std::result::Result<T, ErrorCode>;

#[derive(Debug)]
pub enum ErrorCode {
    /// Error return by arrow
    ArrowError(ArrowError),

    IoError(io::Error),

    NoSuchField,

    ColumnNotExists(String),

    LogicalError(String),

    NoSuchTable(String),

    ParserError(ParserError),

    IntervalError(String),

    PlanError(String),

    NoMatchFunction(String),

    NotSupported(String),

    NotImplemented,
    #[allow(unused)]
    Others,
}

impl From<ArrowError> for ErrorCode {
    fn from(e: ArrowError) -> Self {
        ErrorCode::ArrowError(e)
    }
}

impl From<io::Error> for ErrorCode {
    fn from(e: io::Error) -> Self {
        ErrorCode::IoError(e)
    }
}
impl From<ParserError> for ErrorCode {
    fn from(e: ParserError) -> Self {
        ErrorCode::ParserError(e)
    }
}
