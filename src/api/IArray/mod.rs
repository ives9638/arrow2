mod PrimitiveArray_operator_impl;
mod Utf8Array_operator_impl;
mod BooleanArray_operator_impl;
mod StructArray_operator_impl;

use super::super::error::*;
use crate::api::IValue::types::IValue;
use crate::array::{Array, BooleanArray, PrimitiveArray, Utf8Array};
use crate::datatypes::DataType;
use std::any::Any;
use std::sync::Arc;
pub type I8Array = PrimitiveArray<i8>;
pub type I16Array = PrimitiveArray<i16>;
pub type I32Array = PrimitiveArray<i32>;
pub type I64Array = PrimitiveArray<i64>;
pub type IStrArray = Utf8Array<i32>;
pub type ITextArray = Utf8Array<i64>;
pub type IBoolArray = BooleanArray;
pub type IDate32Array = PrimitiveArray<i32>;
pub type IDate64Array = PrimitiveArray<i64>;

pub struct IArray(pub Arc<dyn ListOperator>);

pub trait ListOperator: Send + Sync + Array {
    fn to_Array(self) -> Box<dyn Array> ;
    #[inline]
    fn get_value(&self, i: usize) -> IValue;
    fn into_array(self) -> IArray
    where
        Self: Sized;
    fn remainder_scalar(&self, rhs: &IValue) -> Result<IArray>;
    fn Sub_scalar(&self, rhs: &IValue) -> Result<IArray>;
    fn Add_scalar(&self, rhs: &IValue) -> Result<IArray>;
    fn Mul_scalar(&self, rhs: &IValue) -> Result<IArray>;
    fn Div_scalar(&self, rhs: &IValue) -> Result<IArray>;

    fn remainder(&self, rhs: &IArray) -> Result<IArray>;
    fn Sub(&self, rhs: &IArray) -> Result<IArray>;
    fn Add(&self, rhs: &IArray) -> Result<IArray>;
    fn Div(&self, rhs: &IArray) -> Result<IArray>;
    fn Mul(&self, rhs: &IArray) -> Result<IArray>;

    fn Max(&self) -> Result<IValue>;
    fn Min(&self) -> Result<IValue>;
}
