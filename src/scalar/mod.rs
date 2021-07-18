use std::any::Any;

use crate::{array::*, datatypes::*};

mod primitive;
pub use primitive::*;
mod utf8;
pub use utf8::*;
mod binary;
pub use binary::*;
mod boolean;
pub use boolean::*;
mod list;
pub use list::*;
mod null;

use super::error::*;
use crate::api::IValue::types::{IStr, IValue, I32};
use crate::api::IValue::IValue_arithmetics::ScalarOperator;
use crate::bitmap::Bitmap;
use crate::buffer::Buffer;
use crate::datatypes::DataType::{Duration, Int32, Interval, Time32, Time64, Timestamp};
use crate::types::NativeType;
pub use null::*;
use std::convert::TryInto;
use std::ops::Deref;
use std::sync::Arc;

pub trait Scalar: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn is_valid(&self) -> bool;
    fn data_type(&self) -> &DataType;
    fn to_boxed_array(&self, length: usize) -> Box<dyn Array>;
    fn into_value(self) -> IValue
    where
        Self: Sized;
    fn remainder(&self, rhs: &dyn Scalar) -> Result<IValue>;
    fn Sub(&self, rhs: &dyn Scalar) -> Result<IValue>;
    fn Add(&self, rhs: &dyn Scalar) -> Result<IValue>;
    fn Div(&self, rhs: &dyn Scalar) -> Result<IValue>;
    fn Mul(&self, rhs: &dyn Scalar) -> Result<IValue>;
    fn Max(&self, rhs: &dyn Scalar) -> Result<IValue>;
    fn Min(&self, rhs: &dyn Scalar) -> Result<IValue>;
}

#[test]
fn new_scalar_test() {
    let data = vec![Some(1), None, Some(10)];
    let bb = vec![Some("a"), None, Some("c")];
    let a = I32::new(DataType::Int32, Some(23));
    let b = I32::new(DataType::Int32, Some(2));

    let cc = a.into_value();
    let vv = b.into_value();
    let q: Option<i32> = vv.Mul(&cc).unwrap().into();
    println!("{}", q.unwrap())
}
