use crate::{array::*, buffer::Buffer, datatypes::DataType};

use super::Scalar;

use crate::api::Ivalue::Ivalue;
use std::any::Any;
use std::ops::{Add, Div, Mul, Sub};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct BinaryScalar<O: Offset> {
    value: Buffer<u8>,
    is_valid: bool,
    phantom: std::marker::PhantomData<O>,
}

impl<O: Offset> BinaryScalar<O> {
    #[inline]
    pub fn new(v: Option<&[u8]>) -> Self {
        let is_valid = v.is_some();
        O::from_usize(v.map(|x| x.len()).unwrap_or_default()).expect("Too large");
        let value = Buffer::from(v.unwrap_or(&[]));
        Self {
            value,
            is_valid,
            phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn value(&self) -> Option<&[u8]> {
        if self.is_valid() {
            Some(self.value.as_slice())
        } else {
            None
        }
    }
}

impl<O: Offset> Scalar for BinaryScalar<O> {
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
            &DataType::LargeBinary
        } else {
            &DataType::Binary
        }
    }
    #[inline]
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
            Box::new(BinaryArray::<O>::from_data(offsets, values, None))
        } else {
            Box::new(BinaryArray::<O>::new_null(length))
        }
    }
    #[inline]
    fn into_value(self) -> Ivalue
    where
        Self: Sized,
    {
        Ivalue(Arc::new(self))
    }


}
