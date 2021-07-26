use crate::api::prelude::array::*;
use crate::api::prelude::{Arc, DataType};

use crate::api::prelude::cast;
use crate::api::prelude::cast::primitive_to_primitive_1;
use crate::api::types::lib::DowncastError;
use crate::compute::cast::cast;
use crate::datatypes::{Field, IntervalUnit, TimeUnit};
use crate::trusted_len::TrustedLen;
use crate::types::{NativeType, NaturalDataType};
use chrono::format::Item;
use std::cmp::Ordering;
use std::ops::Add;
use std::slice::Iter;

#[derive(Clone, Debug)]
pub enum List {
    Null(Arc<NullArray>),
    Bool(Arc<BooleanArray>),

    U8(Arc<UInt8Array>),
    U16(Arc<UInt16Array>),
    U32(Arc<UInt32Array>),
    U64(Arc<UInt64Array>),

    I8(Arc<Int8Array>),
    I16(Arc<Int16Array>),
    I32(Arc<Int32Array>),
    I64(Arc<Int64Array>),

    F32(Arc<Float32Array>),
    F64(Arc<Float64Array>),

    String(Arc<Utf8Array<i32>>),
    Text(Arc<Utf8Array<i64>>),

    Date32(Arc<Int32Array>),
    Date64(Arc<Int64Array>),

    List(Box<Field>),
    Struct(Vec<Field>),
    Binary(Arc<BinaryArray<i32>>),
}

impl List {
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
impl List {
    pub fn is_str(&self) -> bool {
        matches!(self, Self::String(_))
    }

    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(_))
    }

    pub fn as_text(&self) -> Result<Arc<Utf8Array<i64>>, DowncastError> {
        if let Self::Text(ret) = self {
            Ok(ret.clone())
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
}
impl List {
    pub fn from_vec<I: TrustedLen<Item = T>, T>(iter: I) -> Box<List>
    where
        List: From<PrimitiveArray<T>>,
        T: NativeType + NaturalDataType,
    {
        Box::new(PrimitiveArray::<T>::from_trusted_len_values_iter(iter).into())
    }

    pub fn from_str<I: TrustedLen<Item = O>, O>(iter: I) -> List
    where
        List: From<Utf8Array<i32>>,
        O: AsRef<str>,
    {
        Utf8Array::<i32>::from_trusted_len_values_iter(iter).into()
    }
}

impl<T> From<Box<T>> for List
where
    T: Into<Self>,
{
    fn from(value: Box<T>) -> Self {
        (*value).into()
    }
}
impl From<StructArray> for List {
    fn from(value: StructArray) -> Self {
        todo!()
    }
}

impl From<Utf8Array<i64>> for List {
    fn from(value: Utf8Array<i64>) -> Self {
        Self::Text(Arc::new(value))
    }
}
impl From<Utf8Array<i32>> for List {
    fn from(value: Utf8Array<i32>) -> Self {
        Self::String(Arc::new(value))
    }
}
impl From<NullArray> for List {
    fn from(value: NullArray) -> Self {
        Self::Null(Arc::new(value))
    }
}
impl From<Float32Array> for List {
    fn from(value: Float32Array) -> Self {
        Self::F32(Arc::new(value))
    }
}

impl From<Float64Array> for List {
    fn from(value: Float64Array) -> Self {
        Self::F64(Arc::new(value))
    }
}

impl From<UInt8Array> for List {
    fn from(value: UInt8Array) -> Self {
        Self::U8(Arc::new(value))
    }
}
impl From<UInt16Array> for List {
    fn from(value: UInt16Array) -> Self {
        Self::U16(Arc::new(value))
    }
}
impl From<UInt32Array> for List {
    fn from(value: UInt32Array) -> Self {
        Self::U32(Arc::new(value))
    }
}

impl From<UInt64Array> for List {
    fn from(value: UInt64Array) -> Self {
        Self::U64(Arc::new(value))
    }
}

impl From<Int16Array> for List {
    fn from(value: Int16Array) -> Self {
        Self::I16(Arc::new(value))
    }
}

impl From<Int32Array> for List {
    fn from(value: Int32Array) -> Self {
        Self::I32(Arc::new(value))
    }
}
impl From<Int64Array> for List {
    fn from(value: Int64Array) -> Self {
        Self::I64(Arc::new(value))
    }
}
impl From<Int8Array> for List {
    fn from(value: Int8Array) -> Self {
        Self::I8(Arc::new(value))
    }
}
impl From<BooleanArray> for List {
    fn from(value: BooleanArray) -> Self {
        Self::Bool(Arc::new(value))
    }
}
