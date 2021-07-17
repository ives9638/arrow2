use super::super::super::error::*;
use crate::api::IArray::{IArray, ListOperator};
use crate::api::IValue::types::IValue;
use crate::api::IValue::IValue_arithmetics::ScalarOperator;
use crate::array::{Array, PrimitiveArray};
use crate::compute::aggregate::{max_primitive, min_primitive, SimdOrd};
use crate::compute::arithmetics::basic::add::*;
use crate::compute::arithmetics::basic::div::*;
use crate::compute::arithmetics::basic::mul::*;
use crate::compute::arithmetics::basic::rem::{rem, rem_scalar};
use crate::compute::arithmetics::basic::sub::*;
use crate::compute::arithmetics::basic::*;
use crate::io::ipc::gen::Schema::List;
use crate::scalar::{PrimitiveScalar, Scalar};
use crate::types::simd::Simd;
use crate::types::NativeType;
use num::Zero;
use std::sync::Arc;
use ArrowError;

impl<
        T: NativeType
            + Simd
            + std::ops::Rem<Output = T>
            + std::ops::Add<Output = T>
            + std::ops::Sub<Output = T>
            + std::ops::Mul<Output = T>
            + std::ops::Div<Output = T>
            + PartialOrd
            + Zero,
    > ListOperator for PrimitiveArray<T>
where
    T: NativeType + Simd,
    T::Simd: SimdOrd<T>,
{
    fn to_Array(self) -> Box<dyn Array> {
        Box::new(self)
    }
    fn get_value(&self, i: usize) -> IValue {
        let v = match self.is_valid(i) {
            true => Some(self.value(i)),
            false => None,
        };
        let scalar = PrimitiveScalar::<T>::new(self.data_type().clone(), v);
        scalar.into_value()
    }

    fn into_array(self) -> IArray
    where
        Self: Sized,
    {
        IArray(Arc::new(self))
    }

    fn remainder_scalar(&self, rhs: &IValue) -> Result<IArray> {
        if rhs.data_type() != self.data_type() {
            return Err(ArrowError::InvalidArgumentError(
                "Arrays must have the same logical type".to_string(),
            ));
        }
        let v = rhs.0.as_any().downcast_ref::<PrimitiveScalar<T>>().unwrap();
        Ok(rem_scalar(self, &v.value().unwrap()).into_array())
    }

    fn Sub_scalar(&self, rhs: &IValue) -> Result<IArray> {
        if self.data_type() != rhs.data_type() {
            return Err(ArrowError::InvalidArgumentError(
                "Arrays must have the same logical type".to_string(),
            ));
        }
        let v = rhs.0.as_any().downcast_ref::<PrimitiveScalar<T>>().unwrap();
        Ok(sub_scalar(self, &v.value().unwrap()).into_array())
    }

    fn Add_scalar(&self, rhs: &IValue) -> Result<IArray> {
        if self.data_type() != rhs.data_type() {
            return Err(ArrowError::InvalidArgumentError(
                "Arrays must have the same logical type".to_string(),
            ));
        }
        let v = rhs.0.as_any().downcast_ref::<PrimitiveScalar<T>>().unwrap();
        Ok(add_scalar(self, &v.value().unwrap()).into_array())
    }

    fn Mul_scalar(&self, rhs: &IValue) -> Result<IArray> {
        if self.data_type() != rhs.data_type() {
            return Err(ArrowError::InvalidArgumentError(
                "Arrays must have the same logical type".to_string(),
            ));
        }
        let v = rhs.0.as_any().downcast_ref::<PrimitiveScalar<T>>().unwrap();
        Ok(mul_scalar(self, &v.value().unwrap()).into_array())
    }

    fn Div_scalar(&self, rhs: &IValue) -> Result<IArray> {
        if self.data_type() != rhs.data_type() {
            return Err(ArrowError::InvalidArgumentError(
                "Arrays must have the same logical type".to_string(),
            ));
        }
        let v = rhs.0.as_any().downcast_ref::<PrimitiveScalar<T>>().unwrap();
        Ok(div_scalar(self, &v.value().unwrap()).into_array())
    }

    fn remainder(&self, rhs: &IArray) -> Result<IArray> {
        let v = rhs.0.as_any().downcast_ref().unwrap();
        Ok(rem(self, v).unwrap().into_array())
    }

    fn Sub(&self, rhs: &IArray) -> Result<IArray> {
        let v = rhs.0.as_any().downcast_ref().unwrap();
        Ok(sub(self, v).unwrap().into_array())
    }

    fn Add(&self, rhs: &IArray) -> Result<IArray> {
        let v = rhs.0.as_any().downcast_ref().unwrap();
        Ok(add(self, v).unwrap().into_array())
    }

    fn Div(&self, rhs: &IArray) -> Result<IArray> {
        let v = rhs.0.as_any().downcast_ref().unwrap();
        Ok(div(self, v).unwrap().into_array())
    }

    fn Mul(&self, rhs: &IArray) -> Result<IArray> {
        let v = rhs.0.as_any().downcast_ref().unwrap();
        Ok(mul(self, v).unwrap().into_array())
    }

    fn Max(&self) -> Result<IValue> {
        let max = max_primitive(self);
        let max = PrimitiveScalar::<T>::new(self.data_type().clone(), max);
        Ok(max.into_value())
    }

    fn Min(&self) -> Result<IValue> {
        let min = min_primitive(self);
        let min = PrimitiveScalar::<T>::new(self.data_type().clone(), min);
        Ok(min.into_value())
    }
}
