use crate::{array::*, buffer::Buffer, datatypes::DataType, types::NativeType};

use super::Scalar;

use super::super::error::*;
use crate::api::IValue::types::IValue;
use num::{Num, NumCast, Zero};
use std::any::Any;
use std::marker::PhantomData;
use std::ops::{Add, Div, Mul, Rem, Sub};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PrimitiveScalar<T: NativeType> {
    // Not Option<T> because this offers a stabler pointer offset on the struct
    value: T,
    is_valid: bool,
    data_type: DataType,
    t: PhantomData<T>,
}

impl<T: NativeType> PrimitiveScalar<T> {
    #[inline]
    pub fn new(data_type: DataType, v: Option<T>) -> Self {
        let is_valid = v.is_some();
        Self {
            value: v.unwrap_or_default(),
            is_valid,
            data_type,
            t: PhantomData::<T>,
        }
    }

    #[inline]
    pub fn value(&self) -> Option<T> {
        if self.is_valid {
            Some(self.value)
        } else {
            None
        }
    }
}

impl<
        T: NativeType
            + std::ops::Rem<Output = T>
            + std::ops::Add<Output = T>
            + std::ops::Sub<Output = T>
            + std::ops::Mul<Output = T>
            + std::ops::Div<Output = T>
            + PartialOrd
            + Zero,
    > Scalar for PrimitiveScalar<T>
{
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.is_valid
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    fn to_boxed_array(&self, length: usize) -> Box<dyn Array> {
        if self.is_valid {
            let values = Buffer::from_trusted_len_iter(std::iter::repeat(self.value).take(length));
            Box::new(PrimitiveArray::from_data(
                self.data_type.clone(),
                values,
                None,
            ))
        } else {
            Box::new(PrimitiveArray::<T>::new_null(
                self.data_type.clone(),
                length,
            ))
        }
    }
    #[inline]
    fn into_value(self) -> IValue
    where
        Self: Sized,
    {
        IValue(Arc::new(self))
    }
    #[inline]
    fn remainder(&self, rhs: &Scalar) -> Result<IValue> {
        let obj = rhs.as_any().downcast_ref::<Self>().unwrap();
        let r = match (self.value(), obj.value()) {
            (Some(a), Some(b)) => PrimitiveScalar::new(self.data_type().clone(), Some(a % b)),
            _ => PrimitiveScalar::new(self.data_type().clone(), None),
        };
        Ok(r.into_value())
    }

    fn Sub(&self, rhs: &Scalar) -> Result<IValue> {
        let obj = rhs.as_any().downcast_ref::<Self>().unwrap();
        let r = match (self.value(), obj.value()) {
            (Some(a), Some(b)) => PrimitiveScalar::new(self.data_type().clone(), Some(a - b)),
            _ => PrimitiveScalar::new(self.data_type().clone(), None),
        };
        Ok(r.into_value())
    }

    fn Add(&self, rhs: &Scalar) -> Result<IValue> {
        let obj = rhs.as_any().downcast_ref::<Self>().unwrap();
        let r = match (self.value(), obj.value()) {
            (Some(a), Some(b)) => PrimitiveScalar::new(self.data_type().clone(), Some(a + b)),
            _ => PrimitiveScalar::new(self.data_type().clone(), None),
        };
        Ok(r.into_value())
    }

    fn Div(&self, rhs: &Scalar) -> Result<IValue> {
        let obj = rhs.as_any().downcast_ref::<Self>().unwrap();
        let r = match (self.value(), obj.value()) {
            (Some(a), Some(b)) => PrimitiveScalar::new(self.data_type().clone(), Some(a / b)),
            _ => PrimitiveScalar::new(self.data_type().clone(), None),
        };
        Ok(r.into_value())
    }

    fn Mul(&self, rhs: &Scalar) -> Result<IValue> {
        let obj = rhs.as_any().downcast_ref::<Self>().unwrap();
        let r = match (self.value(), obj.value()) {
            (Some(a), Some(b)) => PrimitiveScalar::new(self.data_type().clone(), Some(a * b)),
            _ => PrimitiveScalar::new(self.data_type().clone(), None),
        };
        Ok(r.into_value())
    }

    fn Max(&self, rhs: &Scalar) -> Result<IValue> {
        let obj = rhs.as_any().downcast_ref::<Self>().unwrap();
        let r = match (self.value(), obj.value()) {
            (Some(a), Some(b)) => {
                let v = if a > b { a } else { b };
                PrimitiveScalar::new(self.data_type().clone(), Some(v))
            }
            _ => PrimitiveScalar::new(self.data_type().clone(), None),
        };
        Ok(r.into_value())
    }

    fn Min(&self, rhs: &Scalar) -> Result<IValue> {
        let obj = rhs.as_any().downcast_ref::<Self>().unwrap();
        let r = match (self.value(), obj.value()) {
            (Some(a), Some(b)) => {
                let v = if a < b { a } else { b };
                PrimitiveScalar::new(self.data_type().clone(), Some(v))
            }
            _ => PrimitiveScalar::new(self.data_type().clone(), None),
        };
        Ok(r.into_value())
    }
}
