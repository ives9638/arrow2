use crate::{array::*, datatypes::DataType};

use super::Scalar;

use std::sync::Arc;
use std::ops::Add;
use std::any::Any;
use crate::api::IValue::types::IValue;

#[derive(Debug, Clone)]
pub struct NullScalar {
    phantom: std::marker::PhantomData<bool>,
}

impl NullScalar {
    #[inline]
    pub fn new() -> Self {
        Self { phantom: std::marker::PhantomData}
    }
}

impl Default for NullScalar {
    fn default() -> Self {
        Self::new()
    }
}

impl Scalar for NullScalar {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn is_valid(&self) -> bool {
        false
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &DataType::Null
    }

    #[inline]
    fn to_boxed_array(&self, length: usize) -> Box<dyn Array> {
        Box::new(NullArray::from_data(length))
    }

    fn remainder(&self, rhs: &Scalar) -> crate::error::Result<IValue> {
        todo!()
    }
    fn into_value(self) -> IValue where Self: Sized {
        IValue(Arc::new(self))
    }

    fn Sub(&self, rhs: &Scalar) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Add(&self, rhs: &Scalar) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Div(&self, rhs: &Scalar) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Mul(&self, rhs: &Scalar) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Max(&self, rhs: &Scalar) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Min(&self, rhs: &Scalar) -> crate::error::Result<IValue> {
        todo!()
    }
}
