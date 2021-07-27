//! Everything you need to get started with this crate.
pub use std::sync::Arc;

pub use crate::error::*;
// columns
pub use crate::compute::*;
pub use crate::datatypes::DataType;
pub use crate::datatypes::Schema;
pub use crate::record_batch::*;
pub use crate::array;
pub type DataSchemaRef = Arc<Schema>;
