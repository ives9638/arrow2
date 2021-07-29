use crate::api::prelude::array::*;
use crate::api::prelude::{array, Arc, ArrowError, DataType};

use crate::api::types::lib::DowncastError;

use crate::datatypes::IntervalUnit;
use crate::trusted_len::TrustedLen;
use crate::types::{NativeType, NaturalDataType};
use flatbuffers::EndianScalar;
use futures::future::ok;
use itertools::Itertools;

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
macro_rules! pack_to {
    ($name:ident,$ty:ident) => {
       pub fn $name(list: &List, packed_value: $ty, pos: usize) -> Vec<$ty> {
                match list {
                    Self::Bool(_value) => _value
                        .values()
                        .iter()
                        .map(|val| packed_value | (val as $ty) << (pos))
                        .collect_vec(),
                    Self::I8(_value) => _value
                        .values()
                        .iter()
                        .map(|val| packed_value | (*val as $ty) << (pos))
                        .collect_vec(),
                    Self::I16(_value) => _value
                        .values()
                        .iter()
                        .map(|val| packed_value | (*val as $ty) << (pos))
                        .collect_vec(),
                    Self::I32(_value) => _value
                        .values()
                        .iter()
                        .map(|val| packed_value | (*val as $ty) << (pos))
                        .collect_vec(),
                    Self::I64(_value) => _value
                        .values()
                        .iter()
                        .map(|val| packed_value | (*val as $ty) << (pos))
                        .collect_vec(),
                    Self::U8(_value) => _value
                        .values()
                        .iter()
                        .map(|val| packed_value | (*val as $ty) << (pos))
                        .collect_vec(),
                    Self::U16(_value) => _value
                        .values()
                        .iter()
                        .map(|val| packed_value | (*val as $ty) << (pos))
                        .collect_vec(),
                    Self::U32(_value) => _value
                        .values()
                        .iter()
                        .map(|val| packed_value | (*val as $ty) << (pos))
                        .collect_vec(),
                    Self::U64(_value) => _value
                        .values()
                        .iter()
                        .map(|val| packed_value | (*val as $ty) << (pos))
                        .collect_vec(),

                    Self::Date32(_value) => _value
                        .values()
                        .iter()
                        .map(|val| packed_value | (*val as $ty) << (pos))
                        .collect_vec(),
                    Self::Date64(_value) => _value
                        .values()
                        .iter()
                        .map(|val| packed_value | (*val as $ty) << (pos))
                        .collect_vec(),

                    _ => {
                        todo!()
                    }
                }
            }

    };
}


impl List {
    #[inline]
    pub fn new_empty(data_type: DataType) -> Self {
        match data_type {
            DataType::Null => List::Null(Arc::new(NullArray::new_empty())),
            DataType::Boolean => List::Bool(Arc::new(BooleanArray::new_empty())),
            DataType::Int8 => List::I8(Arc::new(Int8Array::new_empty(data_type))),
            DataType::Int16 => List::I16(Arc::new(Int16Array::new_empty(data_type))),
            DataType::Int32
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Interval(IntervalUnit::YearMonth) => {
                List::I32(Arc::new(Int32Array::new_empty(data_type)))
            }
            DataType::Int64
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_) => List::I64(Arc::new(Int64Array::new_empty(data_type))),

            DataType::UInt8 => List::U8(Arc::new(UInt8Array::new_empty(data_type))),
            DataType::UInt16 => List::U16(Arc::new(UInt16Array::new_empty(data_type))),
            DataType::UInt32 => List::U32(Arc::new(UInt32Array::new_empty(data_type))),
            DataType::UInt64 => List::U64(Arc::new(UInt64Array::new_empty(data_type))),
            DataType::Float16 => unreachable!(),
            DataType::Float32 => List::F32(Arc::new(Float32Array::new_empty(data_type))),
            DataType::Float64 => List::F64(Arc::new(Float64Array::new_empty(data_type))),
            DataType::Binary => List::Binary(Arc::new(BinaryArray::<i32>::new_empty())),
            DataType::LargeBinary => unreachable!(),
            DataType::FixedSizeBinary(_) => unreachable!(),
            DataType::Utf8 => List::String(Arc::new(Utf8Array::<i32>::new_empty())),
            DataType::LargeUtf8 => List::Text(Arc::new(Utf8Array::<i64>::new_empty())),
            DataType::List(_) => List::List(Arc::new(ListArray::<i32>::new_empty(data_type))),
            DataType::LargeList(_) => unreachable!(),
            DataType::FixedSizeList(_, _) => unreachable!(),
            DataType::Struct(fields) => {
                List::Struct(Arc::new(StructArray::new_empty(fields.as_slice())))
            }
            DataType::Union(_) => unimplemented!(),
            DataType::Dictionary(key_type, value_type) => unimplemented!(),
            _ => unimplemented!(),
        }
    }

    #[inline]
    pub fn get_array_ref(self) -> ArrayRef {
        match self {
            Self::Bool(_value) => _value as ArrayRef,
            Self::I8(_value) => _value as ArrayRef,
            Self::I16(_value) => _value as ArrayRef,
            Self::I32(_value) => _value as ArrayRef,
            Self::I64(_value) => _value as ArrayRef,
            Self::U8(_value) => _value as ArrayRef,
            Self::U16(_value) => _value as ArrayRef,
            Self::U32(_value) => _value as ArrayRef,
            Self::U64(_value) => _value as ArrayRef,
            Self::F32(_value) => _value as ArrayRef,
            Self::F64(_value) => _value as ArrayRef,
            Self::String(_value) => _value as ArrayRef,
            Self::Text(_value) => _value as ArrayRef,
            Self::Date32(_value) => _value as ArrayRef,
            Self::Date64(_value) => _value as ArrayRef,
            Self::List(_value) => _value as ArrayRef,
            Self::Struct(_value) => _value as ArrayRef,
            Self::Binary(_value) => _value as ArrayRef,
            _ => {
                todo!()
            }
        }
    }
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
    #[inline]
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
impl List {
    pack_to!(pack_to_u128,u128);
    pack_to!(pack_to_u64,u64);
    pack_to!(pack_to_u32,u32);
    pack_to!(pack_to_u8,u8);
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
        Self::Struct(Arc::new(_value))
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
