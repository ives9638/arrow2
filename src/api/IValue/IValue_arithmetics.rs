use super::super::super::error::*;
use crate::api::IValue::types::IValue;
use crate::datatypes::DataType;

pub trait ScalarOperator {
    fn data_type(&self) -> &DataType;
    fn remainder(&self, rhs: &IValue) -> Result<IValue>;
    fn Sub(&self, rhs: &IValue) -> Result<IValue>;
    fn Add(&self, rhs: &IValue) -> Result<IValue>;
    fn Div(&self, rhs: &IValue) -> Result<IValue>;
    fn Mul(&self, rhs: &IValue) -> Result<IValue>;
    fn Max(&self, rhs: &IValue) -> Result<IValue>;
    fn Min(&self, rhs: &IValue) -> Result<IValue>;
}
impl ScalarOperator for IValue {
    fn data_type(&self) -> &DataType {
        self.0.data_type()
    }

    fn remainder(&self, rhs: &IValue) -> Result<IValue> {
        self.0.remainder(rhs.0.as_ref())
    }

    fn Sub(&self, rhs: &IValue) -> Result<IValue> {
        self.0.Sub(rhs.0.as_ref())
    }

    fn Add(&self, rhs: &IValue) -> Result<IValue> {
        self.0.Add(rhs.0.as_ref())
    }

    fn Div(&self, rhs: &IValue) -> Result<IValue> {
        self.0.Div(rhs.0.as_ref())
    }

    fn Mul(&self, rhs: &IValue) -> Result<IValue> {
        self.0.Mul(rhs.0.as_ref())
    }

    fn Max(&self, rhs: &IValue) -> Result<IValue> {
        self.0.Max(rhs.0.as_ref())
    }

    fn Min(&self, rhs: &IValue) -> Result<IValue> {
        self.0.Min(rhs.0.as_ref())
    }
}
