mod descriptor;
mod reader;
mod sections;
pub mod tables;

pub use descriptor::*;
pub use reader::*;
pub use sections::*;
pub use tables::{TableBEntry, TableDEntry, Tables};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Table error: {0}")]
    Table(String),
    #[error("Invalid data: {0}")]
    Invalid(String),
    #[error("Not supported: {0}")]
    NotSupported(String),
    #[error("Fatal error: {0}")]
    Fatal(String),
}
