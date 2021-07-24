use crate::api::prelude::array::*;
use crate::api::prelude::DataType;

use crate::api::prelude::cast;
use crate::api::types::islist;
use crate::api::types::lib::DowncastError;
use crate::compute::cast::cast;
use crate::datatypes::{Field, IntervalUnit, TimeUnit};
use crate::trusted_len::TrustedLen;
use crate::types::{NativeType, NaturalDataType};
use std::cmp::Ordering;
use std::ops::Add;

impl islist for ValList {}
#[derive(Clone, Debug)]
pub enum ValList {
    Null(NullArray),
    BoolVec(BooleanArray),

    U8Vec(UInt8Array),
    U16Vec(UInt16Array),
    U32Vec(UInt32Array),
    U64Vec(UInt64Array),

    I8Vec(Int8Array),
    I16Vec(Int16Array),
    I32Vec(Int32Array),
    I64Vec(Int64Array),

    F32Vec(Float32Array),
    F64Vec(Float64Array),

    String(Utf8Array<i32>),
    Text(Utf8Array<i64>),

    Date32(Int32Array),
    Date64(Int64Array),

    List(Box<Field>),
    Struct(Vec<Field>),
    Binary(BinaryArray<i32>),
}
macro_rules! cast_to {
    ($self:ident,$dt:ident,$it:ident,$me:ident) => {
        {
             if cast::can_cast_types($self.data_type(), &$dt) {
                match $self {
                    Self::I8Vec(_value) => Ok(cast::primitive_to_primitive::<i8, $it>(
                        &_value,
                        &DataType::$dt,
                    )),
                    Self::I16Vec(_value) => Ok(cast::primitive_to_primitive::<i16, $it>(
                        &_value,
                        &DataType::$dt,
                    )),
                    Self::I32Vec(_value) => Ok(cast::primitive_to_primitive::<i32, $it>(
                        &_value,
                        &DataType::$dt,
                    )),
                    Self::I64Vec(_value) => Ok(cast::primitive_to_primitive::<i64, $it>(
                        &_value,
                        &DataType::$dt,
                    )),
                    Self::U8Vec(_value) => Ok(cast::primitive_to_primitive::<u8, $it>(
                        &_value,
                        &DataType::$dt,
                    )),
                    Self::U16Vec(_value) => Ok(cast::primitive_to_primitive::<u16, $it>(
                        &_value,
                        &DataType::$dt,
                    )),
                    Self::U32Vec(_value) => Ok(cast::primitive_to_primitive::<u32, $it>(
                        &_value,
                        &DataType::$dt,
                    )),
                    Self::U64Vec(_value) => Ok(cast::primitive_to_primitive::<u64, $it>(
                        &_value,
                        &DataType::$dt,
                    )),
                    Self::F32Vec(_value) => Ok(cast::primitive_to_primitive::<f32, $it>(
                        &_value,
                        &DataType::$dt,
                    )),
                    Self::F64Vec(_value) => Ok(cast::primitive_to_primitive::<f64, $it>(
                        &_value,
                        &DataType::$dt,
                    )),
                    Self::String(_value) => {
                        Ok(cast::utf8_to_primitive(&_value, &DataType::$dt))
                    }
                    _ => {
                        Err(DowncastError {
                            from: $self.type_name(),
                            to: "$me",
                        })
                    }
                }
            } else {
                Err(DowncastError {
                    from: $self.type_name(),
                    to: "$me",
                })
            }
        }
                    
        
    };
}
impl ValList  {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::BoolVec(_value) => "boolArray",

            Self::I8Vec(_value) => "Int8Array",
            Self::I16Vec(_value) => "Int16Array",
            Self::I32Vec(_value) => "Int32Array",
            Self::I64Vec(_value) => "Int64Array",

            Self::U8Vec(_value) => "UInt8Array",
            Self::U16Vec(_value) => "UInt16Array",
            Self::U32Vec(_value) => "UInt32Array",
            Self::U64Vec(_value) => "UInt64Array",

            Self::F32Vec(_value) => "Float32Array",
            Self::F64Vec(_value) => "Float64Array",

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
            Self::BoolVec(_value) => _value.data_type(),

            Self::I8Vec(_value) => _value.data_type(),
            Self::I16Vec(_value) => _value.data_type(),
            Self::I32Vec(_value) => _value.data_type(),
            Self::I64Vec(_value) => _value.data_type(),

            Self::U8Vec(_value) => _value.data_type(),
            Self::U16Vec(_value) => _value.data_type(),
            Self::U32Vec(_value) => _value.data_type(),
            Self::U64Vec(_value) => _value.data_type(),

            Self::F32Vec(_value) => _value.data_type(),
            Self::F64Vec(_value) => _value.data_type(),

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
    pub fn is_boolVec(&self) -> bool {
        matches!(self, Self::BoolVec(_))
    }
    pub fn as_boolVec(&self) -> Result<&BooleanArray, DowncastError> {
        if let Self::BoolVec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "BoolVec",
            })
        }
    }
    pub fn into_boolVec(self) -> Result<BooleanArray, DowncastError> {

        if let Self::BoolVec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "BoolVec",
            })
        }
    }
    pub fn is_i8Vec(&self) -> bool {
        matches!(self, Self::I8Vec(_))
    }
    pub fn as_i8Vec(&self) -> Result<&Int8Array, DowncastError> {
        if let Self::I8Vec(ret) = self {
            Ok(ret)
        } else {
            use crate::api::prelude::DataType::*;
            cast_to!( self,Int8,i8,I8Vec )
        }
    }
    pub fn into_i8Vec(self) -> Result<Int8Array, DowncastError> {
        if let Self::I8Vec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "I8Vec",
            })
        }

    }
    pub fn is_i16Vec(&self) -> bool {
        matches!(self, Self::I16Vec(_))
    }
    pub fn as_i16Vec(&self) -> Result<&Int16Array, DowncastError> {
        if let Self::I16Vec(ret) = self {
            Ok(ret)
        } else {
            use crate::api::prelude::DataType::*;
            cast_to!( self,Int16,i16,I16Vec )
        }
    }
    pub fn into_i16Vec(self) -> Result<Int16Array, DowncastError> {
        if let Self::I16Vec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "I16Vec",
            })
        }

    }
    pub fn is_i32Vec(&self) -> bool {
        matches!(self, Self::I32Vec(_))
    }
    pub fn as_i32Vec(&self) -> Result<&Int32Array, DowncastError> {
        if let Self::I32Vec(ret) = self {
            Ok(ret)
        } else {
            use crate::api::prelude::DataType::*;
            cast_to!( self,Int32,i32,I32Vec )
        }
    }
    pub fn into_i32Vec(self) -> Result<Int32Array, DowncastError> {
        if let Self::I32Vec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "I32Vec",
            })
        }

    }
    pub fn is_i64Vec(&self) -> bool {
        matches!(self, Self::I64Vec(_))
    }
    pub fn as_i64Vec(&self) -> Result<&Int64Array, DowncastError> {
        if let Self::I64Vec(ret) = self {
            Ok(ret)
        } else {
            use crate::api::prelude::DataType::*;
            cast_to!( self,Int64,i64,I64Vec )
        }
    }
    pub fn into_i64Vec(self) -> Result<Int64Array, DowncastError> {

        if let Self::I64Vec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "I64Vec",
            })
        }
    }
    pub fn is_u64Vec(&self) -> bool {
        matches!(self, Self::U64Vec(_))
    }
    pub fn as_u64Vec(&self) -> Result<&UInt64Array, DowncastError> {
        if let Self::U64Vec(ret) = self {
            Ok(ret)
        } else {
            use crate::api::prelude::DataType::*;
            cast_to!( self,UInt64,u64,U64Vec )
        }
    }
    pub fn into_u64Vec(self) -> Result<UInt64Array, DowncastError> {
        if let Self::U64Vec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "U64Vec",
            })
        }

    }
    pub fn is_u8Vec(&self) -> bool {
        matches!(self, Self::U8Vec(_))
    }
    pub fn as_u8Vec(&self) -> Result<&UInt8Array, DowncastError> {
        if let Self::U8Vec(ret) = self {
            Ok(ret)
        } else {
            use crate::api::prelude::DataType::*;
            cast_to!( self,UInt8,u8,U8Vec )
        }
    }
    pub fn into_u8Vec(self) -> Result<UInt8Array, DowncastError> {
        if let Self::U8Vec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "U8Vec",
            })
        }
    }
    pub fn is_u16Vec(&self) -> bool {
        matches!(self, Self::U16Vec(_))
    }
    pub fn as_u16Vec(&self) -> Result<&UInt16Array, DowncastError> {
        if let Self::U16Vec(ret) = self {
            Ok(ret)
        } else {
            use crate::api::prelude::DataType::*;
            cast_to!( self,UInt16,u16,U16Vec )
        }
    }
    pub fn into_u16Vec(self) -> Result<UInt16Array, DowncastError> {
        if let Self::U16Vec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "U16Vec",
            })
        }
    }
    pub fn is_u32Vec(&self) -> bool {
        matches!(self, Self::U32Vec(_))
    }
    pub fn as_u32Vec(&self) -> Result<&UInt32Array, DowncastError> {
        if let Self::U32Vec(ret) = self {
            Ok(ret)
        } else {
            use crate::api::prelude::DataType::*;
            cast_to!( self,UInt32,u32,U32Vec )
        }
    }
    pub fn into_u32Vec(self) -> Result<UInt32Array, DowncastError> {
        if let Self::U32Vec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "U32Vec",
            })
        }

    }
    pub fn is_f32Vec(&self) -> bool {
        matches!(self, Self::F32Vec(_))
    }
    pub fn as_f32Vec(&self) -> Result<&Float32Array, DowncastError> {
        if let Self::F32Vec(ret) = self {
            Ok(ret)
        } else {
            use crate::api::prelude::DataType::*;
            cast_to!( self,Float32,f32,f32Vec )
        }
    }
    pub fn into_f32Vec(self) -> Result<Float32Array, DowncastError> {

        if let Self::F32Vec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "F32Vec",
            })
        }
    }
    pub fn is_f64Vec(&self) -> bool {
        matches!(self, Self::F64Vec(_))
    }
    pub fn as_f64Vec(&self) -> Result<&Float64Array, DowncastError> {
        if let Self::F64Vec(ret) = self {
            Ok(ret)
        } else {
            use crate::api::prelude::DataType::*;
            cast_to!( self,Float64,f64,f64Vec )
        }
    }

    pub fn into_f64Vec(self) -> Result<Float64Array, DowncastError> {
        if let Self::F64Vec(ret) = self {
            Ok(ret)
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "F64Vec",
            })
        }
    }
}
impl ValList  {

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
    pub fn from_vec<I: TrustedLen<Item = T>, T>(iter: I) -> ValList
    where
        ValList: From<PrimitiveArray<T>>,
        T: NativeType + NaturalDataType,
    {
        PrimitiveArray::<T>::from_trusted_len_values_iter(iter).into()
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
        Self::F32Vec(value)
    }
}

impl From<Float64Array> for ValList {
    fn from(value: Float64Array) -> Self {
        Self::F64Vec(value)
    }
}

impl From<UInt8Array> for ValList {
    fn from(value: UInt8Array) -> Self {
        Self::U8Vec(value)
    }
}
impl From<UInt16Array> for ValList {
    fn from(value: UInt16Array) -> Self {
        Self::U16Vec(value)
    }
}
impl From<UInt32Array> for ValList {
    fn from(value: UInt32Array) -> Self {
        Self::U32Vec(value)
    }
}

impl From<UInt64Array> for ValList {
    fn from(value: UInt64Array) -> Self {
        Self::U64Vec(value)
    }
}

impl From<Int16Array> for ValList {
    fn from(value: Int16Array) -> Self {
        Self::I16Vec(value)
    }
}

impl From<Int32Array> for ValList {
    fn from(value: Int32Array) -> Self {
        Self::I32Vec(value)
    }
}
impl From<Int64Array> for ValList {
    fn from(value: Int64Array) -> Self {
        Self::I64Vec(value)
    }
}
impl From<Int8Array> for ValList {
    fn from(value: Int8Array) -> Self {
        Self::I8Vec(value)
    }
}
