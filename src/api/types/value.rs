use crate::api::prelude::array::*;
use crate::api::prelude::DataType;
use crate::api::types::lib::DowncastError;
use crate::datatypes::{Field, IntervalUnit, TimeUnit};
use crate::trusted_len::TrustedLen;
use crate::types::{NativeType, NaturalDataType};
use std::cmp::Ordering;

use crate::api::compute::cast::value::isval;

#[derive(Clone, PartialEq,Debug)]
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
        }
    }

    pub fn is_bool(&self) -> bool {
        matches!(self, Self::Bool(_))
    }

  /*  pub fn as_bool(&self) -> Result<bool, DowncastError> {
        if let Self::Bool(ret) = self {
            Ok(*ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "bool",
            })
        }
    }

    pub fn into_bool(self) -> Result<bool, DowncastError> {
        if let Self::Bool(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "bool",
            })
        }
    }

    pub fn is_u8(&self) -> bool {
        matches!(self, Self::U8(_))
    }

    pub fn as_u8(&self) -> Result<u8, DowncastError> {
        if let Self::U8(ret) = self {
            Ok(*ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u8",
            })
        }
    }

    pub fn into_u8(self) -> Result<u8, DowncastError> {
        if let Self::U8(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u8",
            })
        }
    }

    pub fn is_i8(&self) -> bool {
        matches!(self, Self::I8(_))
    }

    pub fn as_i8(&self) -> Result<i8, DowncastError> {
        if let Self::I8(ret) = self {
            Ok(*ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i8",
            })
        }
    }

    pub fn into_i8(self) -> Result<i8, DowncastError> {
        if let Self::I8(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i8",
            })
        }
    }

    pub fn is_u16(&self) -> bool {
        matches!(self, Self::U16(_))
    }

    pub fn as_u16(&self) -> Result<u16, DowncastError> {
        if let Self::U16(ret) = self {
            Ok(*ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u16",
            })
        }
    }

    pub fn into_u16(self) -> Result<u16, DowncastError> {
        if let Self::U16(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u16",
            })
        }
    }

    pub fn is_i16(&self) -> bool {
        matches!(self, Self::I16(_))
    }

    pub fn as_i16(&self) -> Result<i16, DowncastError> {
        if let Self::I16(ret) = self {
            Ok(*ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i16",
            })
        }
    }

    pub fn into_i16(self) -> Result<i16, DowncastError> {
        if let Self::I16(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i16",
            })
        }
    }

    pub fn is_u32(&self) -> bool {
        matches!(self, Self::U32(_))
    }

    pub fn as_u32(&self) -> Result<u32, DowncastError> {
        if let Self::U32(ret) = self {
            Ok(*ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u32",
            })
        }
    }

    pub fn into_u32(self) -> Result<u32, DowncastError> {
        if let Self::U32(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u32",
            })
        }
    }

    pub fn is_i32(&self) -> bool {
        matches!(self, Self::I32(_))
    }

    pub fn as_i32(&self) -> Result<i32, DowncastError> {
        if let Self::I32(ret) = self {
            Ok(*ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i32",
            })
        }
    }

    pub fn into_i32(self) -> Result<i32, DowncastError> {
        if let Self::I32(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i32",
            })
        }
    }

    pub fn is_u64(&self) -> bool {
        matches!(self, Self::U64(_))
    }

    pub fn as_u64(&self) -> Result<u64, DowncastError> {
        if let Self::U64(ret) = self {
            Ok(*ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u64",
            })
        }
    }

    pub fn into_u64(self) -> Result<u64, DowncastError> {
        if let Self::U64(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u64",
            })
        }
    }

    pub fn is_i64(&self) -> bool {
        matches!(self, Self::I64(_))
    }

    pub fn as_i64(&self) -> Result<i64, DowncastError> {
        if let Self::I64(ret) = self {
            Ok(*ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i64",
            })
        }
    }

    pub fn into_i64(self) -> Result<i64, DowncastError> {
        if let Self::I64(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i64",
            })
        }
    }

    pub fn is_f32(&self) -> bool {
        matches!(self, Self::F32(_))
    }

    pub fn as_f32(&self) -> Result<f32, DowncastError> {
        if let Self::F32(ret) = self {
            Ok(*ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "f32",
            })
        }
    }

    pub fn into_f32(self) -> Result<f32, DowncastError> {
        if let Self::F32(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "f32",
            })
        }
    }

    pub fn is_f64(&self) -> bool {
        matches!(self, Self::F64(_))
    }

    pub fn as_f64(&self) -> Result<f64, DowncastError> {
        if let Self::F64(ret) = self {
            Ok(*ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "f64",
            })
        }
    }

    pub fn into_f64(self) -> Result<f64, DowncastError> {
        if let Self::F64(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "f64",
            })
        }
    }*/
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
            _ => None,
        }
    }
}