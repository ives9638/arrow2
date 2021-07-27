#![allow(dead_code)]
#![allow(unused_imports)]
#[warn(unused_variables)]
use crate::api::prelude::arithmetics::negate;
use crate::api::prelude::{ArrowError, Result};
use crate::api::types::list::List;

impl List {
    pub fn negate(&self) -> Result<Self> {
        match self {
            Self::I8(_value) => Ok(negate(_value).into()),
            Self::I16(_value) => Ok(negate(_value).into()),
            Self::I32(_value) => Ok(negate(_value).into()),
            Self::I64(_value) => Ok(negate(_value).into()),
            
            Self::F32(_value) => Ok(negate(_value).into()),
            Self::F64(_value) => Ok(negate(_value).into()),

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
}
