use crate::api::compute::value::Isval;
use crate::api::prelude::array::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use crate::api::prelude::DataType;

use crate::array::{BooleanArray, Utf8Array};
use crate::datatypes::DataType::UInt8;
use crate::types::NativeType;
use std::cmp::Ordering;
use crate::api::List;

#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Bool(bool),
    U8(u8),
    I8(i8),
    U16(u16),
    I16(i16),
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(String),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Bool(_value) => "bool",
            Self::U8(_value) => "u8",
            Self::I8(_value) => "i8",
            Self::U16(_value) => "u16",
            Self::I16(_value) => "i16",
            Self::U32(_value) => "u32",
            Self::I32(_value) => "i32",
            Self::U64(_value) => "u64",
            Self::I64(_value) => "i64",
            Self::F32(_value) => "f32",
            Self::F64(_value) => "f64",
            Self::Str(_value) => "string",
        }
    }

    pub fn into_list(&self, size: usize) -> List {
        match self {
            Self::Bool(_value) => {
                BooleanArray::from_trusted_len_iter(vec![Some(*_value); size].into_iter()).into()
            }
            Self::U8(_value) => {
                UInt8Array::from_trusted_len_iter(vec![Some(*_value); size].into_iter()).into()
            }
            Self::I8(_value) => {
                Int8Array::from_trusted_len_iter(vec![Some(*_value); size].into_iter()).into()
            }
            Self::U16(_value) => {
                UInt16Array::from_trusted_len_iter(vec![Some(*_value); size].into_iter()).into()
            }
            Self::I16(_value) => {
                Int16Array::from_trusted_len_iter(vec![Some(*_value); size].into_iter()).into()
            }
            Self::U32(_value) => {
                UInt32Array::from_trusted_len_iter(vec![Some(*_value); size].into_iter()).into()
            }
            Self::I32(_value) => {
                Int32Array::from_trusted_len_iter(vec![Some(*_value); size].into_iter()).into()
            }
            Self::U64(_value) => {
                UInt64Array::from_trusted_len_iter(vec![Some(*_value); size].into_iter()).into()
            }
            Self::I64(_value) => {
                Int64Array::from_trusted_len_iter(vec![Some(*_value); size].into_iter()).into()
            }
            Self::F32(_value) => {
                Float32Array::from_trusted_len_iter(vec![Some(*_value); size].into_iter()).into()
            }
            Self::F64(_value) => {
                Float64Array::from_trusted_len_iter(vec![Some(*_value); size].into_iter()).into()
            }
            Self::Str(_value) => {
                Utf8Array::<i32>::from_trusted_len_iter(vec![Some(_value); size].into_iter()).into()
            }
        }
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        match self {
            Self::Bool(_value) => &DataType::Boolean,

            Self::I8(_value) => &DataType::Int8,
            Self::I16(_value) => &DataType::Int16,
            Self::I32(_value) => &DataType::Int32,
            Self::I64(_value) => &DataType::Int64,

            Self::U8(_value) => &DataType::UInt8,
            Self::U16(_value) => &DataType::UInt16,
            Self::U32(_value) => &DataType::UInt32,
            Self::U64(_value) => &DataType::UInt64,

            Self::F32(_value) => &DataType::Float32,
            Self::F64(_value) => &DataType::Float64,

            Self::Str(_value) => &DataType::Utf8,

            _ => {
                todo!()
            }
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}
impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Self::U8(value)
    }
}
impl From<i8> for Value {
    fn from(value: i8) -> Self {
        Self::I8(value)
    }
}
impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Self::U16(value)
    }
}
impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Self::I16(value)
    }
}
impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self::U32(value)
    }
}
impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::I32(value)
    }
}
impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}
impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}
impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Self::F32(value)
    }
}
impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Str(value.to_string())
    }
}

impl PartialEq<String> for Value {
    fn eq(&self, other: &String) -> bool {
        self.as_str().map(|strs| &strs == other).unwrap_or(false)
    }
}
impl PartialEq<bool> for Value {
    fn eq(&self, other: &bool) -> bool {
        self.as_bool().map(|bool| &bool == other).unwrap_or(false)
    }
}
impl PartialEq<u8> for Value {
    fn eq(&self, other: &u8) -> bool {
        self.as_u8().map(|u8| &u8 == other).unwrap_or(false)
    }
}
impl PartialEq<i8> for Value {
    fn eq(&self, other: &i8) -> bool {
        self.as_i8().map(|i8| &i8 == other).unwrap_or(false)
    }
}
impl PartialEq<u16> for Value {
    fn eq(&self, other: &u16) -> bool {
        self.as_u16().map(|u16| &u16 == other).unwrap_or(false)
    }
}
impl PartialEq<i16> for Value {
    fn eq(&self, other: &i16) -> bool {
        self.as_i16().map(|i16| &i16 == other).unwrap_or(false)
    }
}
impl PartialEq<u32> for Value {
    fn eq(&self, other: &u32) -> bool {
        self.as_u32().map(|u32| &u32 == other).unwrap_or(false)
    }
}
impl PartialEq<i32> for Value {
    fn eq(&self, other: &i32) -> bool {
        self.as_i32().map(|i32| &i32 == other).unwrap_or(false)
    }
}
impl PartialEq<u64> for Value {
    fn eq(&self, other: &u64) -> bool {
        self.as_u64().map(|u64| &u64 == other).unwrap_or(false)
    }
}
impl PartialEq<i64> for Value {
    fn eq(&self, other: &i64) -> bool {
        self.as_i64().map(|i64| &i64 == other).unwrap_or(false)
    }
}
impl PartialEq<f32> for Value {
    fn eq(&self, other: &f32) -> bool {
        self.as_f32().map(|f32| &f32 == other).unwrap_or(false)
    }
}
impl PartialEq<f64> for Value {
    fn eq(&self, other: &f64) -> bool {
        self.as_f64().map(|f64| &f64 == other).unwrap_or(false)
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a.partial_cmp(b),
            (Self::U8(a), Self::U8(b)) => a.partial_cmp(b),
            (Self::I8(a), Self::I8(b)) => a.partial_cmp(b),
            (Self::U16(a), Self::U16(b)) => a.partial_cmp(b),
            (Self::I16(a), Self::I16(b)) => a.partial_cmp(b),
            (Self::U32(a), Self::U32(b)) => a.partial_cmp(b),
            (Self::I32(a), Self::I32(b)) => a.partial_cmp(b),
            (Self::U64(a), Self::U64(b)) => a.partial_cmp(b),
            (Self::I64(a), Self::I64(b)) => a.partial_cmp(b),
            (Self::F32(a), Self::F32(b)) => a.partial_cmp(b),
            (Self::F64(a), Self::F64(b)) => a.partial_cmp(b),
            (Self::Str(a), Self::Str(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}
