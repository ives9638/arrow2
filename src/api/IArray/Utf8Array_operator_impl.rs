use crate::api::IArray::{IArray, ListOperator};
use crate::api::IValue::types::IValue;
use crate::array::{Array, Offset, Utf8Array};
use crate::compute::concat::concatenate;
use crate::scalar::{PrimitiveScalar, Scalar, Utf8Scalar};
use std::sync::Arc;

impl<O: Offset> ListOperator for Utf8Array<O> {
    fn to_Array(self) -> Box<dyn Array> {
        Box::new(self)
    }

    fn get_value(&self, i: usize) -> IValue {
        let v = match self.is_valid(i) {
            true => Some(self.value(i)),
            false => None,
        };
        let scalar = Utf8Scalar::<O>::new(v);
        scalar.into_value()
    }

    fn into_array(self) -> IArray
    where
        Self: Sized,
    {
        IArray(Arc::new(self))
    }

    fn remainder_scalar(&self, rhs: &IValue) -> crate::error::Result<IArray> {
        todo!()
    }

    fn Sub_scalar(&self, rhs: &IValue) -> crate::error::Result<IArray> {
        todo!()
    }

    fn Add_scalar(&self, rhs: &IValue) -> crate::error::Result<IArray> {
        todo!()
    }

    fn Mul_scalar(&self, rhs: &IValue) -> crate::error::Result<IArray> {
        todo!()
    }

    fn Div_scalar(&self, rhs: &IValue) -> crate::error::Result<IArray> {
        todo!()
    }

    fn remainder(&self, rhs: &IArray) -> crate::error::Result<IArray> {
        todo!()
    }

    fn Sub(&self, rhs: &IArray) -> crate::error::Result<IArray> {
        todo!()
    }

    fn Add(&self, rhs: &IArray) -> crate::error::Result<IArray> {
        let r = rhs.0.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
        Ok(concatenate(&[self, r])?
            .as_any()
            .downcast_ref::<Utf8Array<O>>()
            .unwrap().clone()
            .into_array())
    }

    fn Div(&self, rhs: &IArray) -> crate::error::Result<IArray> {
        todo!()
    }

    fn Mul(&self, rhs: &IArray) -> crate::error::Result<IArray> {
        todo!()
    }

    fn Max(&self) -> crate::error::Result<IValue> {
        todo!()
    }

    fn Min(&self) -> crate::error::Result<IValue> {
        todo!()
    }
}
