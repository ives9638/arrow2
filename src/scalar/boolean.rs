use crate::{array::*, bitmap::Bitmap, datatypes::DataType};

use super::Scalar;

use crate::api::IValue::types::IValue;
use std::any::Any;
use std::ops::Add;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct BooleanScalar {
    value: bool,
    is_valid: bool,
    phantom: std::marker::PhantomData<bool>,
}

impl BooleanScalar {
    #[inline]
    pub fn new(v: Option<bool>) -> Self {
        let is_valid = v.is_some();
        Self {
            value: v.unwrap_or_default(),
            is_valid,
            phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn value(&self) -> Option<bool> {
        if self.is_valid() {
            Some(self.value)
        } else {
            None
        }
    }
}

impl Scalar for BooleanScalar {
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
        &DataType::Boolean
    }

    fn to_boxed_array(&self, length: usize) -> Box<dyn Array> {
        if self.is_valid {
            let values = Bitmap::from_trusted_len_iter(std::iter::repeat(self.value).take(length));
            Box::new(BooleanArray::from_data(values, None))
        } else {
            Box::new(BooleanArray::new_null(length))
        }
    }

    fn into_value(self) -> IValue
    where
        Self: Sized,
    {
        IValue(Arc::new(self))
    }

    fn remainder(&self, rhs: &dyn Scalar) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Sub(&self, rhs: &dyn Scalar) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Add(&self, rhs: &dyn Scalar) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Div(&self, rhs: &dyn Scalar) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Mul(&self, rhs: &dyn Scalar) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Max(&self, rhs: &dyn Scalar) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Min(&self, rhs: &dyn Scalar) -> crate::error::Result<IValue> {
        todo!()
    }
}
