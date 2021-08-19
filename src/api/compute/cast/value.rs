use crate::api::Value;
use crate::api::types::DowncastError;
use std::ops::Deref;

pub trait Isval {
    fn is_bool(&self) -> bool;

    fn as_bool(&self) -> Result<bool, DowncastError>;

    fn into_bool(self) -> Result<bool, DowncastError>;

    fn is_u8(&self) -> bool;

    fn as_u8(&self) -> Result<u8, DowncastError>;

    fn into_u8(self) -> Result<u8, DowncastError>;

    fn is_i8(&self) -> bool;

    fn as_i8(&self) -> Result<i8, DowncastError>;

    fn into_i8(self) -> Result<i8, DowncastError>;

    fn is_u16(&self) -> bool;

    fn as_u16(&self) -> Result<u16, DowncastError>;

    fn into_u16(self) -> Result<u16, DowncastError>;

    fn is_i16(&self) -> bool;
    fn as_i16(&self) -> Result<i16, DowncastError>;

    fn into_i16(self) -> Result<i16, DowncastError>;

    fn is_u32(&self) -> bool;

    fn as_u32(&self) -> Result<u32, DowncastError>;

    fn into_u32(self) -> Result<u32, DowncastError>;

    fn is_i32(&self) -> bool;

    fn as_i32(&self) -> Result<i32, DowncastError>;

    fn into_i32(self) -> Result<i32, DowncastError>;

    fn is_u64(&self) -> bool;

    fn as_u64(&self) -> Result<u64, DowncastError>;

    fn into_u64(self) -> Result<u64, DowncastError>;

    fn is_i64(&self) -> bool;

    fn as_i64(&self) -> Result<i64, DowncastError>;

    fn into_i64(self) -> Result<i64, DowncastError>;

    fn is_f32(&self) -> bool;

    fn as_f32(&self) -> Result<f32, DowncastError>;

    fn into_f32(self) -> Result<f32, DowncastError>;

    fn is_f64(&self) -> bool;

    fn as_f64(&self) -> Result<f64, DowncastError>;

    fn into_f64(self) -> Result<f64, DowncastError>;

    fn is_str(&self) -> bool;

    fn as_str(&self) -> Result<String, DowncastError>;
}

impl Isval for Value {
    fn is_bool(&self) -> bool {
        matches!(self, Self::Bool(_))
    }

    fn as_bool(&self) -> Result<bool, DowncastError> {
        if let Self::Bool(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "bool",
            })
        }
    }

    fn into_bool(self) -> Result<bool, DowncastError> {
        if let Self::Bool(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "bool",
            })
        }
    }

    fn is_u8(&self) -> bool {
        matches!(self, Self::U8(_))
    }

    fn as_u8(&self) -> Result<u8, DowncastError> {
        if let Self::U8(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u8",
            })
        }
    }

    fn into_u8(self) -> Result<u8, DowncastError> {
        if let Self::U8(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u8",
            })
        }
    }

    fn is_i8(&self) -> bool {
        matches!(self, Self::I8(_))
    }

    fn as_i8(&self) -> Result<i8, DowncastError> {
        if let Self::I8(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i8",
            })
        }
    }

    fn into_i8(self) -> Result<i8, DowncastError> {
        if let Self::I8(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i8",
            })
        }
    }

    fn is_u16(&self) -> bool {
        matches!(self, Self::U16(_))
    }

    fn as_u16(&self) -> Result<u16, DowncastError> {
        if let Self::U16(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u16",
            })
        }
    }

    fn into_u16(self) -> Result<u16, DowncastError> {
        if let Self::U16(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u16",
            })
        }
    }

    fn is_i16(&self) -> bool {
        matches!(self, Self::I16(_))
    }

    fn as_i16(&self) -> Result<i16, DowncastError> {
        if let Self::I16(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i16",
            })
        }
    }

    fn into_i16(self) -> Result<i16, DowncastError> {
        if let Self::I16(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i16",
            })
        }
    }

    fn is_u32(&self) -> bool {
        matches!(self, Self::U32(_))
    }

    fn as_u32(&self) -> Result<u32, DowncastError> {
        if let Self::U32(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u32",
            })
        }
    }

    fn into_u32(self) -> Result<u32, DowncastError> {
        if let Self::U32(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u32",
            })
        }
    }

    fn is_i32(&self) -> bool {
        matches!(self, Self::I32(_))
    }

    fn as_i32(&self) -> Result<i32, DowncastError> {
        if let Self::I32(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i32",
            })
        }
    }

    fn into_i32(self) -> Result<i32, DowncastError> {
        if let Self::I32(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i32",
            })
        }
    }

    fn is_u64(&self) -> bool {
        matches!(self, Self::U64(_))
    }

    fn as_u64(&self) -> Result<u64, DowncastError> {
        if let Self::U64(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u64",
            })
        }
    }

    fn into_u64(self) -> Result<u64, DowncastError> {
        if let Self::U64(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "u64",
            })
        }
    }

    fn is_i64(&self) -> bool {
        matches!(self, Self::I64(_))
    }

    fn as_i64(&self) -> Result<i64, DowncastError> {
        if let Self::I64(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i64",
            })
        }
    }

    fn into_i64(self) -> Result<i64, DowncastError> {
        if let Self::I64(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "i64",
            })
        }
    }

    fn is_f32(&self) -> bool {
        matches!(self, Self::F32(_))
    }

    fn as_f32(&self) -> Result<f32, DowncastError> {
        if let Self::F32(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "f32",
            })
        }
    }

    fn into_f32(self) -> Result<f32, DowncastError> {
        if let Self::F32(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "f32",
            })
        }
    }

    fn is_f64(&self) -> bool {
        matches!(self, Self::F64(_))
    }

    fn as_f64(&self) -> Result<f64, DowncastError> {
        if let Self::F64(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "f64",
            })
        }
    }

    fn into_f64(self) -> Result<f64, DowncastError> {
        if let Self::F64(ret) = self {
            Ok(ret.unwrap_or_default())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "f64",
            })
        }
    }

    fn is_str(&self) -> bool {
        matches!(self, Self::Str(_))
    }

    fn as_str(&self) -> Result<String, DowncastError> {
        if let Self::Str(ret) = self {

            Ok(ret.deref().clone().unwrap())
        } else {
            Err(DowncastError {
                from: self.type_name(),
                to: "string",
            })
        }
    }
}
