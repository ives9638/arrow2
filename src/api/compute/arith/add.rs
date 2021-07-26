use crate::api::compute::cast::list::Islist;
use crate::api::compute::cast::value::Isval;
use crate::api::prelude::arithmetics::ArrayAdd;
use crate::api::prelude::ArrowError;
use crate::api::types::list::List;
use crate::api::types::DowncastError;
use crate::api::types::value::Value;

impl ArrayAdd<List> for List
{
    type Output = Self;

    fn add(&self, rhs: &List) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => Ok(_value.add(rhs.as_i8().unwrap().as_ref())?.into()),
            Self::I16(_value) => Ok(_value.add(rhs.as_i16().unwrap().as_ref())?.into()),
            Self::I32(_value) => Ok(_value.add(rhs.as_i32().unwrap().as_ref())?.into()),
            Self::I64(_value) => Ok(_value.add(rhs.as_i64().unwrap().as_ref())?.into()),

            Self::U8(_value) => Ok(_value.add(rhs.as_u8().unwrap().as_ref())?.into()),
            Self::U16(_value) => Ok(_value.add(rhs.as_u16().unwrap().as_ref())?.into()),
            Self::U32(_value) => Ok(_value.add(rhs.as_u32().unwrap().as_ref())?.into()),
            Self::U64(_value) => Ok(_value.add(rhs.as_u64().unwrap().as_ref())?.into()),

            Self::F32(_value) => Ok(_value.add(rhs.as_f32().unwrap().as_ref())?.into()),
            Self::F64(_value) => Ok(_value.add(rhs.as_f64().unwrap().as_ref())?.into()),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
}
impl ArrayAdd<Value> for List

{
    type Output = Self;

    fn add(&self, rhs: &Value) -> crate::api::prelude::Result<Self::Output> {
        match self {
            Self::I8(_value) => Ok(_value.add(&rhs.as_i8().unwrap())?.into()),
            Self::I16(_value) => Ok(_value.add(&rhs.as_i16().unwrap())?.into()),
            Self::I32(_value) => Ok(_value.add(&rhs.as_i32().unwrap())?.into()),
            Self::I64(_value) => Ok(_value.add(&rhs.as_i64().unwrap())?.into()),

            Self::U8(_value) => Ok(_value.add(&rhs.as_u8().unwrap())?.into()),
            Self::U16(_value) => Ok(_value.add(&rhs.as_u16().unwrap())?.into()),
            Self::U32(_value) => Ok(_value.add(&rhs.as_u32().unwrap())?.into()),
            Self::U64(_value) => Ok(_value.add(&rhs.as_u64().unwrap())?.into()),

            Self::F32(_value) => Ok(_value.add(&rhs.as_f32().unwrap())?.into()),
            Self::F64(_value) => Ok(_value.add(&rhs.as_f64().unwrap())?.into()),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<Value>(),
                self.data_type()
            ))),
        }
    }
}
