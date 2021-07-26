use crate::api::prelude::{ArrowError, Result};
use crate::api::types::list::List;
use crate::api::types::value::Value;
use crate::compute::aggregate::*;

impl List {
    pub fn sum(&self) -> Result<Value> {
        match self {
            Self::I8(_value) => Ok(Value::from(sum(&_value).unwrap_or_default())),
            Self::I16(_value) => Ok(Value::from(sum(&_value).unwrap_or_default())),
            Self::I32(_value) => Ok(Value::from(sum(&_value).unwrap_or_default())),
            Self::I64(_value) => Ok(Value::from(sum(&_value).unwrap_or_default())),

            Self::U8(_value) => Ok(Value::from(sum(&_value).unwrap_or_default())),
            Self::U16(_value) => Ok(Value::from(sum(&_value).unwrap_or_default())),
            Self::U32(_value) => Ok(Value::from(sum(&_value).unwrap_or_default())),
            Self::U64(_value) => Ok(Value::from(sum(&_value).unwrap_or_default())),

            Self::F32(_value) => Ok(Value::from(sum(&_value).unwrap_or_default())),
            Self::F64(_value) => Ok(Value::from(sum(&_value).unwrap_or_default())),

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
    pub fn max(&self) -> Result<Value> {
        match self {
            Self::I8(_value) => Ok(Value::from(max_primitive(&_value).unwrap_or_default())),
            Self::I16(_value) => Ok(Value::from(max_primitive(&_value).unwrap_or_default())),
            Self::I32(_value) => Ok(Value::from(max_primitive(&_value).unwrap_or_default())),
            Self::I64(_value) => Ok(Value::from(max_primitive(&_value).unwrap_or_default())),

            Self::U8(_value) => Ok(Value::from(max_primitive(&_value).unwrap_or_default())),
            Self::U16(_value) => Ok(Value::from(max_primitive(&_value).unwrap_or_default())),
            Self::U32(_value) => Ok(Value::from(max_primitive(&_value).unwrap_or_default())),
            Self::U64(_value) => Ok(Value::from(max_primitive(&_value).unwrap_or_default())),

            Self::F32(_value) => Ok(Value::from(max_primitive(&_value).unwrap_or_default())),
            Self::F64(_value) => Ok(Value::from(max_primitive(&_value).unwrap_or_default())),

            Self::String(_value) => Ok(Value::from(max_string(&_value).unwrap_or_default())),
            Self::Bool(_value) => Ok(Value::from(max_boolean(&_value).unwrap_or_default())),

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
    pub fn min(&self) -> Result<Value> {
        match self {
            Self::I8(_value) => Ok(Value::from(min_primitive(&_value).unwrap_or_default())),
            Self::I16(_value) => Ok(Value::from(min_primitive(&_value).unwrap_or_default())),
            Self::I32(_value) => Ok(Value::from(min_primitive(&_value).unwrap_or_default())),
            Self::I64(_value) => Ok(Value::from(min_primitive(&_value).unwrap_or_default())),

            Self::U8(_value) => Ok(Value::from(min_primitive(&_value).unwrap_or_default())),
            Self::U16(_value) => Ok(Value::from(min_primitive(&_value).unwrap_or_default())),
            Self::U32(_value) => Ok(Value::from(min_primitive(&_value).unwrap_or_default())),
            Self::U64(_value) => Ok(Value::from(min_primitive(&_value).unwrap_or_default())),

            Self::F32(_value) => Ok(Value::from(min_primitive(&_value).unwrap_or_default())),
            Self::F64(_value) => Ok(Value::from(min_primitive(&_value).unwrap_or_default())),

            Self::String(_value) => Ok(Value::from(min_string(&_value).unwrap_or_default())),
            Self::Bool(_value) => Ok(Value::from(min_boolean(&_value).unwrap_or_default())),

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

}
