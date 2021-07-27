
use crate::api::prelude::array::*;
use crate::api::prelude::{Arc, ArrowError, DataType};

use crate::api::types::lib::DowncastError;

use crate::trusted_len::TrustedLen;
use crate::types::{NativeType, NaturalDataType};
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

    List(Arc<ListArray<i32>>),
    Struct(Arc<StructArray>),
    Binary(Arc<BinaryArray<i32>>),
}

impl List {
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Result<List, ArrowError> {
        match self {
            Self::Bool(_value) => Ok(Self::Bool(Arc::new(_value.slice(offset, length)))),
            Self::I8(_value) => Ok(Self::I8(Arc::new(_value.slice(offset, length)))),
            Self::I16(_value) => Ok(Self::I16(Arc::new(_value.slice(offset, length)))),
            Self::I32(_value) => Ok(Self::I32(Arc::new(_value.slice(offset, length)))),
            Self::I64(_value) => Ok(Self::I64(Arc::new(_value.slice(offset, length)))),
            Self::U8(_value) => Ok(Self::U8(Arc::new(_value.slice(offset, length)))),
            Self::U16(_value) => Ok(Self::U16(Arc::new(_value.slice(offset, length)))),
            Self::U32(_value) => Ok(Self::U32(Arc::new(_value.slice(offset, length)))),
            Self::U64(_value) => Ok(Self::U64(Arc::new(_value.slice(offset, length)))),
            Self::F32(_value) => Ok(Self::F32(Arc::new(_value.slice(offset, length)))),
            Self::F64(_value) => Ok(Self::F64(Arc::new(_value.slice(offset, length)))),
            Self::String(_value) => Ok(Self::String(Arc::new(_value.slice(offset, length)))),
            Self::Text(_value) => Ok(Self::Text(Arc::new(_value.slice(offset, length)))),
            Self::Date32(_value) => Ok(Self::Date32(Arc::new(_value.slice(offset, length)))),
            Self::Date64(_value) => Ok(Self::Date64(Arc::new(_value.slice(offset, length)))),
            Self::List(_value) => Ok(Self::List(Arc::new(_value.slice(offset, length)))),
            Self::Struct(_value) => Ok(Self::Struct(Arc::new(_value.slice(offset, length)))),
            Self::Binary(_value) => Ok(Self::Binary(Arc::new(_value.slice(offset, length)))),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Null(_value) => _value.len(),
            Self::Bool(_value) => _value.len(),

            Self::I8(_value) => _value.len(),
            Self::I16(_value) => _value.len(),
            Self::I32(_value) => _value.len(),
            Self::I64(_value) => _value.len(),

            Self::U8(_value) => _value.len(),
            Self::U16(_value) => _value.len(),
            Self::U32(_value) => _value.len(),
            Self::U64(_value) => _value.len(),

            Self::F32(_value) => _value.len(),
            Self::F64(_value) => _value.len(),

            Self::String(_value) => _value.len(),
            Self::Text(_value) => _value.len(),

            Self::Date32(_value) => _value.len(),
            Self::Date64(_value) => _value.len(),

            Self::List(_value) => _value.len(),
            Self::Binary(_value) => _value.len(),
            Self::Struct(_value) => _value.len(),
            _ => 0,
        }
    }
    #[inline]
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
    #[inline]
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
    #[inline]
    pub fn is_str(&self) -> bool {
        matches!(self, Self::String(_))
    }
    #[inline]
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(_))
    }
    #[inline]
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
    #[inline]
    pub fn is_list(&self) -> bool {
        matches!(self, Self::List(_))
    }
    #[inline]
    pub fn as_list(&self) -> Result<Arc<ListArray<i32>>, DowncastError> {
        if let Self::List(ret) = self {
            Ok(ret.clone())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "Struct",
            })
        }
    }

    pub fn is_struct(&self) -> bool {
        matches!(self, Self::Struct(_))
    }
    #[inline]
    pub fn as_struct(&self) -> Result<Arc<StructArray>, DowncastError> {
        if let Self::Struct(ret) = self {
            Ok(ret.clone())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "Struct",
            })
        }
    }
}
impl List {
    pub fn from_vec<I: TrustedLen<Item = T>, T>(iter: I) -> Arc<List>
    where
        List: From<PrimitiveArray<T>>,
        T: NativeType + NaturalDataType,
    {
        Arc::new(PrimitiveArray::<T>::from_trusted_len_values_iter(iter).into())
    }

    pub fn from_str<I: TrustedLen<Item = O>, O>(iter: I) -> Arc<List>
    where
        List: From<Utf8Array<i32>>,
        O: AsRef<str>,
    {
        Arc::new(Utf8Array::<i32>::from_trusted_len_values_iter(iter).into())
    }
}

impl<T> From<Box<T>> for List
where
    T: Into<Self>,
{
    #[inline]
    fn from(value: Box<T>) -> Self {
        (*value).into()
    }
}

impl From<StructArray> for List {
    #[inline]

    fn from(_value: StructArray) -> Self {
        todo!()
    }
}

impl From<Utf8Array<i64>> for List {
    #[inline]
    fn from(value: Utf8Array<i64>) -> Self {
        Self::Text(Arc::new(value))
    }
}
impl From<Utf8Array<i32>> for List {
    #[inline]
    fn from(value: Utf8Array<i32>) -> Self {
        Self::String(Arc::new(value))
    }
}
impl From<NullArray> for List {
    #[inline]
    fn from(value: NullArray) -> Self {
        Self::Null(Arc::new(value))
    }
}
impl From<Float32Array> for List {
    #[inline]
    fn from(value: Float32Array) -> Self {
        Self::F32(Arc::new(value))
    }
}

impl From<Float64Array> for List {
    #[inline]
    fn from(value: Float64Array) -> Self {
        Self::F64(Arc::new(value))
    }
}

impl From<UInt8Array> for List {
    #[inline]
    fn from(value: UInt8Array) -> Self {
        Self::U8(Arc::new(value))
    }
}
impl From<UInt16Array> for List {
    #[inline]
    fn from(value: UInt16Array) -> Self {
        Self::U16(Arc::new(value))
    }
}
impl From<UInt32Array> for List {
    #[inline]
    fn from(value: UInt32Array) -> Self {
        Self::U32(Arc::new(value))
    }
}

impl From<UInt64Array> for List {
    #[inline]
    fn from(value: UInt64Array) -> Self {
        Self::U64(Arc::new(value))
    }
}

impl From<Int16Array> for List {
    #[inline]
    fn from(value: Int16Array) -> Self {
        Self::I16(Arc::new(value))
    }
}

impl From<Int32Array> for List {
    #[inline]
    fn from(value: Int32Array) -> Self {
        Self::I32(Arc::new(value))
    }
}
impl From<Int64Array> for List {
    #[inline]
    fn from(value: Int64Array) -> Self {
        Self::I64(Arc::new(value))
    }
}
impl From<Int8Array> for List {
    #[inline]
    fn from(value: Int8Array) -> Self {
        Self::I8(Arc::new(value))
    }
}
impl From<BooleanArray> for List {
    #[inline]
    fn from(value: BooleanArray) -> Self {
        Self::Bool(Arc::new(value))
    }
}
impl From<ListArray<i32>> for List {
    #[inline]
    fn from(value: ListArray<i32>) -> Self {
        Self::List(Arc::new(value))
    }
}

