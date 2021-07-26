use crate::api::compute::cast::list::Islist;
use crate::api::compute::cast::value::Isval;
use crate::api::compute::compare::ArrayComare;
use crate::api::prelude::array::BooleanArray;
use crate::api::prelude::like::{like_utf8, like_utf8_scalar, nlike_utf8};
use crate::api::types::list::List;
use crate::api::types::value::Value;
use crate::compute::comparison::*;
use crate::compute::like::nlike_utf8_scalar;
use crate::api::prelude::ArrowError;

impl ArrayComare<List> for List {
    type Output = BooleanArray;

    fn Eq(&self, rhs: &List) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::eq(_value, rhs.as_i8().unwrap().as_ref()),
            Self::I16(_value) => primitive::eq(_value, rhs.as_i16().unwrap().as_ref()),
            Self::I32(_value) => primitive::eq(_value, rhs.as_i32().unwrap().as_ref()),
            Self::I64(_value) => primitive::eq(_value, rhs.as_i64().unwrap().as_ref()),

            Self::U8(_value) => primitive::eq(_value, rhs.as_u8().unwrap().as_ref()),
            Self::U16(_value) => primitive::eq(_value, rhs.as_u16().unwrap().as_ref()),
            Self::U32(_value) => primitive::eq(_value, rhs.as_u32().unwrap().as_ref()),
            Self::U64(_value) => primitive::eq(_value, rhs.as_u64().unwrap().as_ref()),

            Self::F32(_value) => primitive::eq(_value, rhs.as_f32().unwrap().as_ref()),
            Self::F64(_value) => primitive::eq(_value, rhs.as_f64().unwrap().as_ref()),

            Self::String(_value) => {
                utf8::compare(_value, rhs.as_str().unwrap().as_ref(), Operator::Eq)
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn Neq(&self, rhs: &List) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::neq(_value, rhs.as_i8().unwrap().as_ref()),
            Self::I16(_value) => primitive::neq(_value, rhs.as_i16().unwrap().as_ref()),
            Self::I32(_value) => primitive::neq(_value, rhs.as_i32().unwrap().as_ref()),
            Self::I64(_value) => primitive::neq(_value, rhs.as_i64().unwrap().as_ref()),

            Self::U8(_value) => primitive::neq(_value, rhs.as_u8().unwrap().as_ref()),
            Self::U16(_value) => primitive::neq(_value, rhs.as_u16().unwrap().as_ref()),
            Self::U32(_value) => primitive::neq(_value, rhs.as_u32().unwrap().as_ref()),
            Self::U64(_value) => primitive::neq(_value, rhs.as_u64().unwrap().as_ref()),

            Self::F32(_value) => primitive::neq(_value, rhs.as_f32().unwrap().as_ref()),
            Self::F64(_value) => primitive::neq(_value, rhs.as_f64().unwrap().as_ref()),

            Self::String(_value) => {
                utf8::compare(_value, rhs.as_str().unwrap().as_ref(), Operator::Neq)
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn Gt(&self, rhs: &List) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::gt(_value, rhs.as_i8().unwrap().as_ref()),
            Self::I16(_value) => primitive::gt(_value, rhs.as_i16().unwrap().as_ref()),
            Self::I32(_value) => primitive::gt(_value, rhs.as_i32().unwrap().as_ref()),
            Self::I64(_value) => primitive::gt(_value, rhs.as_i64().unwrap().as_ref()),

            Self::U8(_value) => primitive::gt(_value, rhs.as_u8().unwrap().as_ref()),
            Self::U16(_value) => primitive::gt(_value, rhs.as_u16().unwrap().as_ref()),
            Self::U32(_value) => primitive::gt(_value, rhs.as_u32().unwrap().as_ref()),
            Self::U64(_value) => primitive::gt(_value, rhs.as_u64().unwrap().as_ref()),

            Self::F32(_value) => primitive::gt(_value, rhs.as_f32().unwrap().as_ref()),
            Self::F64(_value) => primitive::gt(_value, rhs.as_f64().unwrap().as_ref()),

            Self::String(_value) => {
                utf8::compare(_value, rhs.as_str().unwrap().as_ref(), Operator::Gt)
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn GtEq(&self, rhs: &List) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::gt_eq(_value, rhs.as_i8().unwrap().as_ref()),
            Self::I16(_value) => primitive::gt_eq(_value, rhs.as_i16().unwrap().as_ref()),
            Self::I32(_value) => primitive::gt_eq(_value, rhs.as_i32().unwrap().as_ref()),
            Self::I64(_value) => primitive::gt_eq(_value, rhs.as_i64().unwrap().as_ref()),

            Self::U8(_value) => primitive::gt_eq(_value, rhs.as_u8().unwrap().as_ref()),
            Self::U16(_value) => primitive::gt_eq(_value, rhs.as_u16().unwrap().as_ref()),
            Self::U32(_value) => primitive::gt_eq(_value, rhs.as_u32().unwrap().as_ref()),
            Self::U64(_value) => primitive::gt_eq(_value, rhs.as_u64().unwrap().as_ref()),

            Self::F32(_value) => primitive::gt_eq(_value, rhs.as_f32().unwrap().as_ref()),
            Self::F64(_value) => primitive::gt_eq(_value, rhs.as_f64().unwrap().as_ref()),

            Self::String(_value) => {
                utf8::compare(_value, rhs.as_str().unwrap().as_ref(), Operator::GtEq)
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn Lt(&self, rhs: &List) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::lt(_value, rhs.as_i8().unwrap().as_ref()),
            Self::I16(_value) => primitive::lt(_value, rhs.as_i16().unwrap().as_ref()),
            Self::I32(_value) => primitive::lt(_value, rhs.as_i32().unwrap().as_ref()),
            Self::I64(_value) => primitive::lt(_value, rhs.as_i64().unwrap().as_ref()),

            Self::U8(_value) => primitive::lt(_value, rhs.as_u8().unwrap().as_ref()),
            Self::U16(_value) => primitive::lt(_value, rhs.as_u16().unwrap().as_ref()),
            Self::U32(_value) => primitive::lt(_value, rhs.as_u32().unwrap().as_ref()),
            Self::U64(_value) => primitive::lt(_value, rhs.as_u64().unwrap().as_ref()),

            Self::F32(_value) => primitive::lt(_value, rhs.as_f32().unwrap().as_ref()),
            Self::F64(_value) => primitive::lt(_value, rhs.as_f64().unwrap().as_ref()),

            Self::String(_value) => {
                utf8::compare(_value, rhs.as_str().unwrap().as_ref(), Operator::Lt)
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn LtEq(&self, rhs: &List) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::lt_eq(_value, rhs.as_i8().unwrap().as_ref()),
            Self::I16(_value) => primitive::lt_eq(_value, rhs.as_i16().unwrap().as_ref()),
            Self::I32(_value) => primitive::lt_eq(_value, rhs.as_i32().unwrap().as_ref()),
            Self::I64(_value) => primitive::lt_eq(_value, rhs.as_i64().unwrap().as_ref()),

            Self::U8(_value) => primitive::lt_eq(_value, rhs.as_u8().unwrap().as_ref()),
            Self::U16(_value) => primitive::lt_eq(_value, rhs.as_u16().unwrap().as_ref()),
            Self::U32(_value) => primitive::lt_eq(_value, rhs.as_u32().unwrap().as_ref()),
            Self::U64(_value) => primitive::lt_eq(_value, rhs.as_u64().unwrap().as_ref()),

            Self::F32(_value) => primitive::lt_eq(_value, rhs.as_f32().unwrap().as_ref()),
            Self::F64(_value) => primitive::lt_eq(_value, rhs.as_f64().unwrap().as_ref()),

            Self::String(_value) => {
                utf8::compare(_value, rhs.as_str().unwrap().as_ref(), Operator::LtEq)
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn like(&self, rhs: &List) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::String(_value) => like_utf8(_value, rhs.as_str().unwrap().as_ref()),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn not_like(&self, rhs: &List) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::String(_value) => nlike_utf8(_value, rhs.as_str().unwrap().as_ref()),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
}

impl ArrayComare<Value> for List {
    type Output = BooleanArray;

    fn Eq(&self, rhs: &Value) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::eq_scalar(_value, rhs.as_i8().unwrap()),
            Self::I16(_value) => primitive::eq_scalar(_value, rhs.as_i16().unwrap()),
            Self::I32(_value) => primitive::eq_scalar(_value, rhs.as_i32().unwrap()),
            Self::I64(_value) => primitive::eq_scalar(_value, rhs.as_i64().unwrap()),

            Self::U8(_value) => primitive::eq_scalar(_value, rhs.as_u8().unwrap()),
            Self::U16(_value) => primitive::eq_scalar(_value, rhs.as_u16().unwrap()),
            Self::U32(_value) => primitive::eq_scalar(_value, rhs.as_u32().unwrap()),
            Self::U64(_value) => primitive::eq_scalar(_value, rhs.as_u64().unwrap()),

            Self::F32(_value) => primitive::eq_scalar(_value, rhs.as_f32().unwrap()),
            Self::F64(_value) => primitive::eq_scalar(_value, rhs.as_f64().unwrap()),

            Self::String(_value) => {
                Ok(utf8::compare_scalar(_value, &rhs.as_str().unwrap(), Operator::Eq))
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn Neq(&self, rhs: &Value) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::neq_scalar(_value, rhs.as_i8().unwrap()),
            Self::I16(_value) => primitive::neq_scalar(_value, rhs.as_i16().unwrap()),
            Self::I32(_value) => primitive::neq_scalar(_value, rhs.as_i32().unwrap()),
            Self::I64(_value) => primitive::neq_scalar(_value, rhs.as_i64().unwrap()),

            Self::U8(_value) => primitive::neq_scalar(_value, rhs.as_u8().unwrap()),
            Self::U16(_value) => primitive::neq_scalar(_value, rhs.as_u16().unwrap()),
            Self::U32(_value) => primitive::neq_scalar(_value, rhs.as_u32().unwrap()),
            Self::U64(_value) => primitive::neq_scalar(_value, rhs.as_u64().unwrap()),

            Self::F32(_value) => primitive::neq_scalar(_value, rhs.as_f32().unwrap()),
            Self::F64(_value) => primitive::neq_scalar(_value, rhs.as_f64().unwrap()),

            Self::String(_value) => {
                Ok(utf8::compare_scalar(_value, &rhs.as_str().unwrap(), Operator::Neq))
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn Gt(&self, rhs: &Value) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::gt_scalar(_value, rhs.as_i8().unwrap()),
            Self::I16(_value) => primitive::gt_scalar(_value, rhs.as_i16().unwrap()),
            Self::I32(_value) => primitive::gt_scalar(_value, rhs.as_i32().unwrap()),
            Self::I64(_value) => primitive::gt_scalar(_value, rhs.as_i64().unwrap()),

            Self::U8(_value) => primitive::gt_scalar(_value, rhs.as_u8().unwrap()),
            Self::U16(_value) => primitive::gt_scalar(_value, rhs.as_u16().unwrap()),
            Self::U32(_value) => primitive::gt_scalar(_value, rhs.as_u32().unwrap()),
            Self::U64(_value) => primitive::gt_scalar(_value, rhs.as_u64().unwrap()),

            Self::F32(_value) => primitive::gt_scalar(_value, rhs.as_f32().unwrap()),
            Self::F64(_value) => primitive::gt_scalar(_value, rhs.as_f64().unwrap()),

            Self::String(_value) => {
                Ok(utf8::compare_scalar(_value, &rhs.as_str().unwrap(), Operator::Gt))
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn GtEq(&self, rhs: &Value) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::gt_eq_scalar(_value, rhs.as_i8().unwrap()),
            Self::I16(_value) => primitive::gt_eq_scalar(_value, rhs.as_i16().unwrap()),
            Self::I32(_value) => primitive::gt_eq_scalar(_value, rhs.as_i32().unwrap()),
            Self::I64(_value) => primitive::gt_eq_scalar(_value, rhs.as_i64().unwrap()),

            Self::U8(_value) => primitive::gt_eq_scalar(_value, rhs.as_u8().unwrap()),
            Self::U16(_value) => primitive::gt_eq_scalar(_value, rhs.as_u16().unwrap()),
            Self::U32(_value) => primitive::gt_eq_scalar(_value, rhs.as_u32().unwrap()),
            Self::U64(_value) => primitive::gt_eq_scalar(_value, rhs.as_u64().unwrap()),

            Self::F32(_value) => primitive::gt_eq_scalar(_value, rhs.as_f32().unwrap()),
            Self::F64(_value) => primitive::gt_eq_scalar(_value, rhs.as_f64().unwrap()),

            Self::String(_value) => {
               Ok( utf8::compare_scalar(_value, &rhs.as_str().unwrap(), Operator::GtEq))
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn Lt(&self, rhs: &Value) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::lt_scalar(_value, rhs.as_i8().unwrap()),
            Self::I16(_value) => primitive::lt_scalar(_value, rhs.as_i16().unwrap()),
            Self::I32(_value) => primitive::lt_scalar(_value, rhs.as_i32().unwrap()),
            Self::I64(_value) => primitive::lt_scalar(_value, rhs.as_i64().unwrap()),

            Self::U8(_value) => primitive::lt_scalar(_value, rhs.as_u8().unwrap()),
            Self::U16(_value) => primitive::lt_scalar(_value, rhs.as_u16().unwrap()),
            Self::U32(_value) => primitive::lt_scalar(_value, rhs.as_u32().unwrap()),
            Self::U64(_value) => primitive::lt_scalar(_value, rhs.as_u64().unwrap()),

            Self::F32(_value) => primitive::lt_scalar(_value, rhs.as_f32().unwrap()),
            Self::F64(_value) => primitive::lt_scalar(_value, rhs.as_f64().unwrap()),

            Self::String(_value) => {
              Ok(  utf8::compare_scalar(_value, &rhs.as_str().unwrap(), Operator::Lt))
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn LtEq(&self, rhs: &Value) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => primitive::lt_eq_scalar(_value, rhs.as_i8().unwrap()),
            Self::I16(_value) => primitive::lt_eq_scalar(_value, rhs.as_i16().unwrap()),
            Self::I32(_value) => primitive::lt_eq_scalar(_value, rhs.as_i32().unwrap()),
            Self::I64(_value) => primitive::lt_eq_scalar(_value, rhs.as_i64().unwrap()),

            Self::U8(_value) => primitive::lt_eq_scalar(_value, rhs.as_u8().unwrap()),
            Self::U16(_value) => primitive::lt_eq_scalar(_value, rhs.as_u16().unwrap()),
            Self::U32(_value) => primitive::lt_eq_scalar(_value, rhs.as_u32().unwrap()),
            Self::U64(_value) => primitive::lt_eq_scalar(_value, rhs.as_u64().unwrap()),

            Self::F32(_value) => primitive::lt_eq_scalar(_value, rhs.as_f32().unwrap()),
            Self::F64(_value) => primitive::lt_eq_scalar(_value, rhs.as_f64().unwrap()),

            Self::String(_value) => {
              Ok ( utf8::compare_scalar(_value, &rhs.as_str().unwrap(), Operator::LtEq))
            }

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn like(&self, rhs: &Value) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::String(_value) => like_utf8_scalar(_value, &rhs.as_str().unwrap()),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }

    fn not_like(&self, rhs: &Value) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::String(_value) => nlike_utf8_scalar(_value, &rhs.as_str().unwrap()),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
}
