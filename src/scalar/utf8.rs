use crate::{array::*, buffer::Buffer, datatypes::DataType};

use super::Scalar;

use super::super::error::*;
use crate::api::IValue::types::IValue;
use std::ops::Add;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Utf8Scalar<O: Offset> {
    value: Buffer<u8>,
    is_valid: bool,
    phantom: std::marker::PhantomData<O>,
}

impl<O: Offset> Utf8Scalar<O> {
    #[inline]
    pub fn new(v: Option<&str>) -> Self {
        let is_valid = v.is_some();
        O::from_usize(v.map(|x| x.len()).unwrap_or_default()).expect("Too large");
        let value = Buffer::from(v.map(|x| x.as_bytes()).unwrap_or(&[]));
        Self {
            value,
            is_valid,
            phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn value(&self) -> Option<&str> {
        if self.is_valid() {
            Some(unsafe { std::str::from_utf8_unchecked(self.value.as_slice()) })
        } else {
            None
        }
    }
}

impl<O: Offset> Scalar for Utf8Scalar<O> {
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
        if O::is_large() {
            &DataType::LargeUtf8
        } else {
            &DataType::Utf8
        }
    }

    fn to_boxed_array(&self, length: usize) -> Box<dyn Array> {
        if self.is_valid {
            let item_length = O::from_usize(self.value.len()).unwrap(); // verified at `new`
            let offsets = (0..=length).map(|i| O::from_usize(i).unwrap() * item_length);
            let offsets = unsafe { Buffer::from_trusted_len_iter_unchecked(offsets) };
            let values = std::iter::repeat(self.value.as_slice())
                .take(length)
                .flatten()
                .copied()
                .collect();
            Box::new(Utf8Array::<O>::from_data(offsets, values, None))
        } else {
            Box::new(Utf8Array::<O>::new_null(length))
        }
    }

    fn into_value(self) -> IValue
    where
        Self: Sized,
    {
        IValue(Arc::new(self))
    }
    fn remainder(&self, rhs: &Scalar) -> Result<IValue> {
        todo!()
    }

    fn Sub(&self, rhs: &Scalar) -> Result<IValue> {
        todo!()
    }

    fn Add(&self, rhs: &Scalar) -> Result<IValue> {
        let obj = rhs.as_any().downcast_ref::<Self>().unwrap();
        match (self.value(), obj.value()) {
            (Some(a), Some(b)) => {
                let obj = rhs.as_any().downcast_ref::<Self>().unwrap();
                let obj = Utf8Scalar::<O>::new(Some((a.to_string() + b).as_str()));
                Ok(obj.into_value())
            }
            (Some(a), None) => {
                let obj = Utf8Scalar::<O>::new(Some(a));
                Ok(obj.into_value())
            }

            (None, Some(b)) => {
                let obj = Utf8Scalar::<O>::new(Some(b));
                Ok(obj.into_value())
            }
            (None, None) => Ok(Utf8Scalar::<O>::new(None).into_value()),
        }
    }

    fn Div(&self, rhs: &Scalar) -> Result<IValue> {
        todo!()
    }

    fn Mul(&self, rhs: &Scalar) -> Result<IValue> {
        todo!()
    }

    fn Max(&self, rhs: &Scalar) -> Result<IValue> {
        todo!()
    }

    fn Min(&self, rhs: &Scalar) -> Result<IValue> {
        todo!()
    }
}
