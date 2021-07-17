use crate::api::IArray::{ListOperator, IArray};
use crate::array::{StructArray, Array};
use crate::api::IValue::types::IValue;

impl  ListOperator for StructArray{
    fn to_Array(self) -> Box<dyn Array> {
        Box::new(self)
    }

    fn get_value(&self, i: usize) -> IValue {
        todo!()
    }

    fn into_array(self) -> IArray where Self: Sized {
        todo!()
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