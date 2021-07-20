use crate::{array::*, buffer::Buffer, datatypes::DataType, types::NativeType};

use super::Scalar;
use super::super::compute::cast;
use super::super::error::*;
use crate::api::IValue::IValue;
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

impl<T: NativeType> Scalar for PrimitiveScalar<T>
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


}
pub type Int8Scalar = PrimitiveScalar<i8>;

pub type Int16Scalar = PrimitiveScalar<i16>;
/// A type definition [`PrimitiveScalar`] for `i32`
pub type Int32Scalar = PrimitiveScalar<i32>;
/// A type definition [`PrimitiveScalar`] for `i64`
pub type Int64Scalar = PrimitiveScalar<i64>;
/// A type definition [`PrimitiveScalar`] for `i128`
pub type Int128Scalar = PrimitiveScalar<i128>;

/// A type definition [`PrimitiveScalar`] for `f32`
pub type Float32Scalar = PrimitiveScalar<f32>;
/// A type definition [`PrimitiveScalar`] for `f64`
pub type Float64Scalar = PrimitiveScalar<f64>;
/// A type definition [`PrimitiveScalar`] for `u8`
pub type UInt8Scalar = PrimitiveScalar<u8>;
/// A type definition [`PrimitiveScalar`] for `u16`
pub type UInt16Scalar = PrimitiveScalar<u16>;
/// A type definition [`PrimitiveScalar`] for `u32`
pub type UInt32Scalar = PrimitiveScalar<u32>;
/// A type definition [`PrimitiveScalar`] for `u64`
pub type UInt64Scalar = PrimitiveScalar<u64>;
