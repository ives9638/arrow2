pub mod value;
pub mod list;
mod lib;
mod value_test;

pub use lib::*;
use crate::api::types::list::List;
use crate::api::types::value::Value;
use crate::api::prelude::Arc;

#[derive(Clone)]
pub enum DataColumn {
    // Array of values.
    Array(Arc<List>),
    // A Single value.
    Constant(Value, usize),
}
