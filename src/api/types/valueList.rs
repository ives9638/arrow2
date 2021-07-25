use crate::api::prelude::array::*;
use crate::api::prelude::DataType;

use crate::api::prelude::cast;
use crate::api::types::lib::DowncastError;
use crate::compute::cast::cast;
use crate::datatypes::{Field, IntervalUnit, TimeUnit};
use crate::trusted_len::TrustedLen;
use crate::types::{NativeType, NaturalDataType};
use std::cmp::Ordering;
use std::ops::Add;
use crate::api::prelude::cast::primitive_to_primitive_1;
 
#[derive(Clone, Debug)]
pub enum ValList {
    Null(NullArray),
    Bool(BooleanArray),

    U8(UInt8Array),
    U16(UInt16Array),
    U32(UInt32Array),
    U64(UInt64Array),

    I8(Int8Array),
    I16(Int16Array),
    I32(Int32Array),
    I64(Int64Array),

    F32(Float32Array),
    F64(Float64Array),

    String(Utf8Array<i32>),
    Text(Utf8Array<i64>),

    Date32(Int32Array),
    Date64(Int64Array),

    List(Box<Field>),
    Struct(Vec<Field>),
    Binary(BinaryArray<i32>),
}
 
impl ValList {
    pub fn type_name(&self) -> &'static str {

        match self {
            Self::Bool(_value) => "boolArray",

            Self::I8(_value) => "Int8Array",
            Self::I16(_value) => "Int16Array",
            Self::I32(_value) => "Int32Array",
            Self::I64(_value) => "Int64Array",

            Self::U8(_value) => "UInt8Array",
            Self::U16(_value) => "UInt16Array",
            Self::U32(_value) => "UInt32Array",
            Self::U64(_value) => "UInt64Array",

            Self::F32(_value) => "Float32Array",
            Self::F64(_value) => "Float64Array",

            Self::String(_value) => "UTFArray(i32)",
            Self::Text(_value) => "UTFArray(i64)",

            Self::Date32(_value) => "Date32",
            Self::Date64(_value) => "Date64",

            Self::List(_value) => "List",
            Self::Struct(_value) => "Struct",
            Self::Binary(_value) => "Binary",
            Self::Null(_value) => "NULL",
        }
    }
    pub fn data_type(&self) -> &DataType {
        match self {
            Self::Bool(_value) => _value.data_type(),

            Self::I8(_value) => _value.data_type(),
            Self::I16(_value) => _value.data_type(),
            Self::I32(_value) => _value.data_type(),
            Self::I64(_value) => _value.data_type(),

            Self::U8(_value) => _value.data_type(),
            Self::U16(_value) => _value.data_type(),
            Self::U32(_value) => _value.data_type(),
            Self::U64(_value) => _value.data_type(),

            Self::F32(_value) => _value.data_type(),
            Self::F64(_value) => _value.data_type(),

            Self::String(_value) => _value.data_type(),
            Self::Text(_value) => _value.data_type(),

            Self::Date32(_value) => _value.data_type(),
            Self::Date64(_value) => _value.data_type(),

            Self::List(_value) => _value.data_type(),
            Self::Binary(_value) => _value.data_type(),
            Self::Null(_value) => _value.data_type(),
            _ => {
                todo!()
            }
        }
    }

}
impl ValList {
    pub fn is_string(&self) -> bool {
        matches!(self, Self::String(_))
    }

    pub fn as_string(&self) -> Result<&Utf8Array<i32>, DowncastError> {
        if let Self::String(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "String",
            })
        }
    }

    pub fn into_string(self) -> Result<Utf8Array<i32>, DowncastError> {
        if let Self::String(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "String",
            })
        }
    }

    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(_))
    }

    pub fn as_text(&self) -> Result<&Utf8Array<i64>, DowncastError> {
        if let Self::Text(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "String",
            })
        }
    }

    pub fn into_text(self) -> Result<Utf8Array<i64>, DowncastError> {
        if let Self::Text(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "String",
            })
        }
    }

    pub fn is_struct(&self) -> bool {
        matches!(self, Self::Struct(_))
    }

    pub fn as_struct(&self) -> Result<&Vec<Field>, DowncastError> {
        if let Self::Struct(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "Struct",
            })
        }
    }

    pub fn into_struct(self) -> Result<Vec<Field>, DowncastError> {
        if let Self::Struct(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "Struct",
            })
        }
    }
}
impl ValList {
    pub fn from_vec<I: TrustedLen<Item = T>, T>(iter: I) ->Box<ValList>
        where
            ValList: From<PrimitiveArray<T>>,
            T: NativeType + NaturalDataType,
    {
       Box::new( PrimitiveArray::<T>::from_trusted_len_values_iter(iter).into())
    }
    pub fn from_str<I: TrustedLen<Item = O>, O>(iter: I) -> ValList
        where
            ValList: From<Utf8Array<i32>>,
            O: AsRef<str>,
    {
        Utf8Array::<i32>::from_trusted_len_values_iter(iter).into()
    }
}

impl<T> From<Box<T>> for ValList
    where
        T: Into<Self>,
{
    fn from(value: Box<T>) -> Self {
        (*value).into()
    }
}
impl From<StructArray> for ValList {
    fn from(value: StructArray) -> Self {
        todo!()
    }
}

impl From<Utf8Array<i64>> for ValList {
    fn from(value: Utf8Array<i64>) -> Self {
        Self::Text(value)
    }
}
impl From<Utf8Array<i32>> for ValList {
    fn from(value: Utf8Array<i32>) -> Self {
        Self::String(value)
    }
}
impl From<NullArray> for ValList {
    fn from(value: NullArray) -> Self {
        Self::Null(value)
    }
}
impl From<Float32Array> for ValList {
    fn from(value: Float32Array) -> Self {
        Self::F32(value)
    }
}

impl From<Float64Array> for ValList {
    fn from(value: Float64Array) -> Self {
        Self::F64(value)
    }
}

impl From<UInt8Array> for ValList {
    fn from(value: UInt8Array) -> Self {
        Self::U8(value)
    }
}
impl From<UInt16Array> for ValList {
    fn from(value: UInt16Array) -> Self {
        Self::U16(value)
    }
}
impl From<UInt32Array> for ValList {
    fn from(value: UInt32Array) -> Self {
        Self::U32(value)
    }
}

impl From<UInt64Array> for ValList {
    fn from(value: UInt64Array) -> Self {
        Self::U64(value)
    }
}

impl From<Int16Array> for ValList {
    fn from(value: Int16Array) -> Self {
        Self::I16(value)
    }
}

impl From<Int32Array> for ValList {
    fn from(value: Int32Array) -> Self {
        Self::I32(value)
    }
}
impl From<Int64Array> for ValList {
    fn from(value: Int64Array) -> Self {
        Self::I64(value)
    }
}
impl From<Int8Array> for ValList {
    fn from(value: Int8Array) -> Self {
        Self::I8(value)
    }
}
