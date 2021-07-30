use crate::api::prelude::*;

use crate::array::Array;
use crate::compute::aggregate::*;

impl List {
    #[inline]
    pub fn get_array_memory_size(&self) -> usize {
        match self {
            Self::Bool(_value) => estimated_bytes_size(_value.as_ref()),
            Self::I8(_value) => estimated_bytes_size(_value.as_ref()),
            Self::I16(_value) => estimated_bytes_size(_value.as_ref()),
            Self::I32(_value) => estimated_bytes_size(_value.as_ref()),
            Self::I64(_value) => estimated_bytes_size(_value.as_ref()),
            Self::U8(_value) => estimated_bytes_size(_value.as_ref()),
            Self::U16(_value) => estimated_bytes_size(_value.as_ref()),
            Self::U32(_value) => estimated_bytes_size(_value.as_ref()),
            Self::U64(_value) => estimated_bytes_size(_value.as_ref()),
            Self::F32(_value) => estimated_bytes_size(_value.as_ref()),
            Self::F64(_value) => estimated_bytes_size(_value.as_ref()),
            Self::Date32(_value) => estimated_bytes_size(_value.as_ref()),
            Self::Date64(_value) => estimated_bytes_size(_value.as_ref()),
            Self::String(_value) => estimated_bytes_size(_value.as_ref()),
            Self::Text(_value) => estimated_bytes_size(_value.as_ref()),

            Self::List(_value) => estimated_bytes_size(_value.as_ref()),
            Self::Struct(_value) => estimated_bytes_size(_value.as_ref()),
            Self::Binary(_value) => estimated_bytes_size(_value.as_ref()),
            Self::Null(_value) => estimated_bytes_size(_value.as_ref()),
        }
    }
    #[inline]
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
    #[inline]
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
    #[inline]
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
