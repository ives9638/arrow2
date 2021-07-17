use crate::api::IArray::{IArray, ListOperator};
use crate::api::IValue::types::{IBool, IValue};
use crate::array::{Array, BooleanArray};
use crate::scalar::Scalar;
use std::sync::Arc;

impl ListOperator for BooleanArray {
    fn to_Array(self) -> Box<dyn Array> {
        Box::new(self)
    }

    fn get_value(&self, i: usize) -> IValue {
        let v = match self.is_valid(i) {
            true => Some(self.value(i)),
            false => None,
        };
        let scalar = IBool::new(v);
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
        todo!()
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
