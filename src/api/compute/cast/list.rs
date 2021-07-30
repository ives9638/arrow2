
use crate::api::prelude::cast::*;
use crate::api::types::DowncastError;
use crate::api::prelude::array::*;
use crate::api::prelude::{Arc, DataType};
use crate::api::List;


pub trait Islist {
    fn as_bool(&self) -> Result<Arc<BooleanArray>, DowncastError>;
    fn as_i8(&self) -> Result<Arc<Int8Array>, DowncastError>;
    fn as_i16(&self) -> Result<Arc<Int16Array>, DowncastError>;
    fn as_i32(&self) -> Result<Arc<Int32Array>, DowncastError>;
    fn as_i64(&self) -> Result<Arc<Int64Array>, DowncastError>;
    fn as_u64(&self) -> Result<Arc<UInt64Array>, DowncastError>;
    fn as_u8(&self) -> Result<Arc<UInt8Array>, DowncastError>;
    fn as_u16(&self) -> Result<Arc<UInt16Array>, DowncastError>;
    fn as_u32(&self) -> Result<Arc<UInt32Array>, DowncastError>;
    fn as_f32(&self) -> Result<Arc<Float32Array>, DowncastError>;
    fn as_f64(&self) -> Result<Arc<Float64Array>, DowncastError>;
    fn as_str(&self) -> Result<Arc<Utf8Array<i32>>, DowncastError>;
}

impl Islist for List {
    fn as_str(&self) -> Result<Arc<Utf8Array<i32>>, DowncastError> {
        if let Self::String(ret) = self {
            return Ok(ret.clone());
        }
        if !can_cast_types(self.data_type(), &DataType::Utf8) {
            return Err(DowncastError {
                from: self.type_name(),
                to: "Utf8",
            });
        }

        match self {
            Self::I8(v) => Ok(Arc::new(primitive_to_utf8(&v))),
            Self::I16(v) => Ok(Arc::new(primitive_to_utf8(&v))),
            Self::I32(v) => Ok(Arc::new(primitive_to_utf8(&v))),
            Self::I64(v) => Ok(Arc::new(primitive_to_utf8(&v))),

            Self::U8(v) => Ok(Arc::new(primitive_to_utf8(&v))),
            Self::U16(v) => Ok(Arc::new(primitive_to_utf8(&v))),
            Self::U32(v) => Ok(Arc::new(primitive_to_utf8(&v))),
            Self::U64(v) => Ok(Arc::new(primitive_to_utf8(&v))),

            Self::F32(v) => Ok(Arc::new(primitive_to_utf8(&v))),
            Self::F64(v) => Ok(Arc::new(primitive_to_utf8(&v))),

            _ => {
                todo!()
            }
        }
    }
    fn as_bool(&self) -> Result<Arc<BooleanArray>, DowncastError> {
        if let Self::Bool(ret) = self {
            Ok(ret.clone())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "BoolVec",
            })
        }
    }
    fn as_i8(&self) -> Result<Arc<Int8Array>, DowncastError> {
        if let Self::I8(ret) = self {
            return Ok(ret.clone());
        }
        if !can_cast_types(self.data_type(), &DataType::Int8) {
            return Err(DowncastError {
                from: self.type_name(),
                to: "I8",
            });
        }
        match self {
            Self::I8(v) => Ok(Arc::new(primitive_to_primitive::<_, i8>(
                &v,
                &DataType::Int8,
            ))),
            Self::String(v) => Ok(Arc::new(utf8_to_primitive(&v, &DataType::Int8))),
            _ => {
                todo!()
            }
        }
    }
    fn as_i16(&self) -> Result<Arc<Int16Array>, DowncastError> {
        if let Self::I16(ret) = self {
            Ok(ret.clone())
        } else {
            if !can_cast_types(self.data_type(), &DataType::Int8) {
                return Err(DowncastError {
                    from: self.type_name(),
                    to: "I16Vec",
                });
            }
            match self {
                Self::I8(v) => Ok(Arc::new(primitive_to_primitive::<_, i16>(
                    &v,
                    &DataType::Int16,
                ))),
                Self::String(v) => Ok(Arc::new(utf8_to_primitive(&v, &DataType::Int16))),

                _ => {
                    todo!()
                }
            }
        }
    }
    fn as_i32(&self) -> Result<Arc<Int32Array>, DowncastError> {
        if let Self::I32(ret) = self {
            Ok(ret.clone())
        } else {
            if !can_cast_types(self.data_type(), &DataType::Int32) {
                return Err(DowncastError {
                    from: self.type_name(),
                    to: "I32",
                });
            }
            match self {
                Self::I8(v) => Ok(Arc::new(primitive_to_primitive::<_, i32>(
                    &v,
                    &DataType::Int32,
                ))),
                Self::I16(v) => Ok(Arc::new(primitive_to_primitive::<_, i32>(
                    &v,
                    &DataType::Int32,
                ))),
                Self::String(v) => Ok(Arc::new(utf8_to_primitive(&v, &DataType::Int32))),
                _ => {
                    todo!()
                }
            }
        }
    }
    fn as_i64(&self) -> Result<Arc<Int64Array>, DowncastError> {
        if let Self::I64(ret) = self {
            Ok(ret.clone())
        } else {
            if !can_cast_types(self.data_type(), &DataType::Int8) {
                return Err(DowncastError {
                    from: self.type_name(),
                    to: "I16Vec",
                });
            }
            match self {
                Self::I8(v) => Ok(Arc::new(primitive_to_primitive::<_, i64>(
                    &v,
                    &DataType::Int64,
                ))),
                Self::I16(v) => Ok(Arc::new(primitive_to_primitive::<_, i64>(
                    &v,
                    &DataType::Int64,
                ))),
                Self::I32(v) => Ok(Arc::new(primitive_to_primitive::<_, i64>(
                    &v,
                    &DataType::Int64,
                ))),
                Self::String(v) => Ok(Arc::new(utf8_to_primitive(&v, &DataType::Int64))),
                _ => {
                    todo!()
                }
            }
        }
    }
    fn as_u64(&self) -> Result<Arc<UInt64Array>, DowncastError> {
        if let Self::U64(ret) = self {
            Ok(ret.clone())
        } else {
            if !can_cast_types(self.data_type(), &DataType::Int8) {
                return Err(DowncastError {
                    from: self.type_name(),
                    to: "I16Vec",
                });
            }
            match self {
                Self::I8(v) => Ok(Arc::new(primitive_to_primitive::<_, u64>(
                    &v,
                    &DataType::UInt64,
                ))),
                Self::I16(v) => Ok(Arc::new(primitive_to_primitive::<_, u64>(
                    &v,
                    &DataType::UInt64,
                ))),
                Self::I32(v) => Ok(Arc::new(primitive_to_primitive::<_, u64>(
                    &v,
                    &DataType::UInt64,
                ))),
                Self::U8(v) => Ok(Arc::new(primitive_to_primitive::<_, u64>(
                    &v,
                    &DataType::UInt64,
                ))),
                Self::U16(v) => Ok(Arc::new(primitive_to_primitive::<_, u64>(
                    &v,
                    &DataType::UInt64,
                ))),
                Self::U32(v) => Ok(Arc::new(primitive_to_primitive::<_, u64>(
                    &v,
                    &DataType::UInt64,
                ))),
                Self::String(v) => Ok(Arc::new(utf8_to_primitive(&v, &DataType::UInt64))),
                _ => {
                    todo!()
                }
            }
        }
    }
    fn as_u8(&self) -> Result<Arc<UInt8Array>, DowncastError> {
        if let Self::U8(ret) = self {
            Ok(ret.clone())
        } else {
            if !can_cast_types(self.data_type(), &DataType::Int8) {
                return Err(DowncastError {
                    from: self.type_name(),
                    to: "I16Vec",
                });
            }
            match self {
                Self::I8(v) => Ok(Arc::new(primitive_to_primitive::<_, u8>(
                    &v,
                    &DataType::UInt8,
                ))),
                Self::I16(v) => Ok(Arc::new(primitive_to_primitive::<_, u8>(
                    &v,
                    &DataType::UInt8,
                ))),
                Self::I32(v) => Ok(Arc::new(primitive_to_primitive::<_, u8>(
                    &v,
                    &DataType::UInt8,
                ))),
                Self::U8(v) => Ok(Arc::new(primitive_to_primitive::<_, u8>(
                    &v,
                    &DataType::UInt8,
                ))),
                Self::U16(v) => Ok(Arc::new(primitive_to_primitive::<_, u8>(
                    &v,
                    &DataType::UInt8,
                ))),
                Self::U32(v) => Ok(Arc::new(primitive_to_primitive::<_, u8>(
                    &v,
                    &DataType::UInt8,
                ))),
                Self::String(v) => Ok(Arc::new(utf8_to_primitive(&v, &DataType::UInt8))),
                _ => {
                    todo!()
                }
            }
        }
    }
    fn as_u16(&self) -> Result<Arc<UInt16Array>, DowncastError> {
        if let Self::U16(ret) = self {
            Ok(ret.clone())
        } else {
            if !can_cast_types(self.data_type(), &DataType::Int8) {
                return Err(DowncastError {
                    from: self.type_name(),
                    to: "I16Vec",
                });
            }
            match self {
                Self::I8(v) => Ok(Arc::new(primitive_to_primitive::<_, u16>(
                    &v,
                    &DataType::UInt16,
                ))),
                Self::I16(v) => Ok(Arc::new(primitive_to_primitive::<_, u16>(
                    &v,
                    &DataType::UInt16,
                ))),
                Self::I32(v) => Ok(Arc::new(primitive_to_primitive::<_, u16>(
                    &v,
                    &DataType::UInt16,
                ))),
                Self::U8(v) => Ok(Arc::new(primitive_to_primitive::<_, u16>(
                    &v,
                    &DataType::UInt16,
                ))),
                Self::U16(v) => Ok(Arc::new(primitive_to_primitive::<_, u16>(
                    &v,
                    &DataType::UInt16,
                ))),
                Self::U32(v) => Ok(Arc::new(primitive_to_primitive::<_, u16>(
                    &v,
                    &DataType::UInt16,
                ))),
                Self::String(v) => Ok(Arc::new(utf8_to_primitive(&v, &DataType::UInt16))),
                _ => {
                    todo!()
                }
            }
        }
    }
    fn as_u32(&self) -> Result<Arc<UInt32Array>, DowncastError> {
        if let Self::U32(ret) = self {
            Ok(ret.clone())
        } else {
            if !can_cast_types(self.data_type(), &DataType::Int8) {
                return Err(DowncastError {
                    from: self.type_name(),
                    to: "I16Vec",
                });
            }
            match self {
                Self::I8(v) => Ok(Arc::new(primitive_to_primitive::<_, u32>(
                    &v,
                    &DataType::UInt32,
                ))),
                Self::I16(v) => Ok(Arc::new(primitive_to_primitive::<_, u32>(
                    &v,
                    &DataType::UInt32,
                ))),
                Self::I32(v) => Ok(Arc::new(primitive_to_primitive::<_, u32>(
                    &v,
                    &DataType::UInt32,
                ))),
                Self::U8(v) => Ok(Arc::new(primitive_to_primitive::<_, u32>(
                    &v,
                    &DataType::UInt32,
                ))),
                Self::U64(v) => Ok(Arc::new(primitive_to_primitive::<_, u32>(
                    &v,
                    &DataType::UInt32,
                ))),
                Self::U32(v) => Ok(Arc::new(primitive_to_primitive::<_, u32>(
                    &v,
                    &DataType::UInt32,
                ))),
                Self::String(v) => Ok(Arc::new(utf8_to_primitive(&v, &DataType::UInt32))),
                _ => {
                    todo!()
                }
            }
        }
    }
    fn as_f32(&self) -> Result<Arc<Float32Array>, DowncastError> {
        if let Self::F32(ret) = self {
            Ok(ret.clone())
        } else {
            if !can_cast_types(self.data_type(), &DataType::Int8) {
                return Err(DowncastError {
                    from: self.type_name(),
                    to: "I16Vec",
                });
            }
            match self {
                Self::I8(v) => Ok(Arc::new(primitive_to_primitive::<_, f32>(
                    &v,
                    &DataType::Float32,
                ))),
                Self::I16(v) => Ok(Arc::new(primitive_to_primitive::<_, f32>(
                    &v,
                    &DataType::Float32,
                ))),
                Self::I32(v) => Ok(Arc::new(primitive_to_primitive::<_, f32>(
                    &v,
                    &DataType::Float32,
                ))),
                Self::U8(v) => Ok(Arc::new(primitive_to_primitive::<_, f32>(
                    &v,
                    &DataType::Float32,
                ))),
                Self::F32(v) => Ok(Arc::new(primitive_to_primitive::<_, f32>(
                    &v,
                    &DataType::Float32,
                ))),
                Self::F64(v) => Ok(Arc::new(primitive_to_primitive::<_, f32>(
                    &v,
                    &DataType::Float32,
                ))),
                Self::String(v) => Ok(Arc::new(utf8_to_primitive(&v, &DataType::Float32))),
                _ => {
                    todo!()
                }
            }
        }
    }
    fn as_f64(&self) -> Result<Arc<Float64Array>, DowncastError> {
        if let Self::F64(ret) = self {
            Ok(ret.clone())
        } else {
            if !can_cast_types(self.data_type(), &DataType::Int8) {
                return Err(DowncastError {
                    from: self.type_name(),
                    to: "I16Vec",
                });
            }
            match self {
                Self::I8(v) => Ok(Arc::new(primitive_to_primitive::<_, f64>(
                    &v,
                    &DataType::Float64,
                ))),
                Self::I16(v) => Ok(Arc::new(primitive_to_primitive::<_, f64>(
                    &v,
                    &DataType::Float64,
                ))),
                Self::I32(v) => Ok(Arc::new(primitive_to_primitive::<_, f64>(
                    &v,
                    &DataType::Float64,
                ))),
                Self::U8(v) => Ok(Arc::new(primitive_to_primitive::<_, f64>(
                    &v,
                    &DataType::Float64,
                ))),
                Self::F32(v) => Ok(Arc::new(primitive_to_primitive::<_, f64>(
                    &v,
                    &DataType::Float64,
                ))),
                Self::String(v) => Ok(Arc::new(utf8_to_primitive(&v, &DataType::Float64))),
                _ => {
                    todo!()
                }
            }
        }
    }
}
