use crate::api::prelude::array::{ UInt64Array};
use crate::api::prelude::{ArrowError, Result};
use crate::api::List;

use crate::compute::hash::{hash_boolean, hash_primitive, hash_utf8};

impl List {
    pub fn hash(&self) -> Result<UInt64Array> {
        match self {
            Self::I8(_value) => Ok(hash_primitive(_value)),
            Self::I16(_value) => Ok(hash_primitive(_value)),
            Self::I32(_value) => Ok(hash_primitive(_value)),
            Self::I64(_value) => Ok(hash_primitive(_value)),

            Self::U8(_value) => Ok(hash_primitive(_value)),
            Self::U16(_value) => Ok(hash_primitive(_value)),
            Self::U32(_value) => Ok(hash_primitive(_value)),
            Self::U64(_value) => Ok(hash_primitive(_value)),


            Self::String(_value) => Ok(hash_utf8(_value)),
            Self::Text(_value) => Ok(hash_utf8(_value)),
            Self::Bool(_value) => Ok(hash_boolean(_value)),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
}
