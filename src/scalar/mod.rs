use std::any::Any;

use crate::{array::*, datatypes::*, types::days_ms};

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

use crate::api::types::{IStr, I32, IntoV};
use crate::bitmap::Bitmap;
use crate::buffer::Buffer;
use crate::datatypes::DataType::{Duration, Int32, Interval, Time32, Time64, Timestamp};
use crate::types::NativeType;
pub use null::*;
use std::convert::TryInto;
use std::ops::Deref;
use std::sync::Arc;

pub trait Scalar {
    fn as_any(&self) -> &dyn Any;
    fn is_valid(&self) -> bool;
    fn data_type(&self) -> &DataType;
    fn to_boxed_array(&self, length: usize) -> Box<dyn Array>;
}

#[test]
fn new_scalar_test() {
    let data = vec![Some(1), None, Some(10)];
    let bb = vec![Some("a"), None, Some("c")];
    let a = I32::new(DataType::Date32, Some(23));
    let b = IStr::new(Some("WW"));

    let cc = a.into_value();
    let vv = b.into_value();
    let q: String = vv.into();

    println!("{}", q)
}
