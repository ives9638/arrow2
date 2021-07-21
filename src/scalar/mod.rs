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

use crate::bitmap::Bitmap;
use crate::buffer::Buffer;
use crate::datatypes::DataType::{Duration, Int32, Interval, Time32, Time64, Timestamp};
use crate::types::NativeType;
pub use null::*;
use std::convert::TryInto;
use std::ops::Deref;
use std::sync::Arc;
use crate::api::Ivalue::Ivalue;
use std::iter::FromIterator;

pub trait Scalar: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn is_valid(&self) -> bool;
    fn data_type(&self) -> &DataType;
    fn to_boxed_array(&self, length: usize) -> Box<dyn Array>;
    fn into_value(self) -> Ivalue
    where
        Self: Sized;
}

#[test]
fn new_scalar_test() {
    let data = vec![Some(1), None, Some(10)];
    let bb = vec![Some("a"), None, Some("c")];
    let a = Int32Scalar::new(DataType::Int32, Some(23));
    let b = Int32Scalar::new(DataType::Int32, Some(2));


    let al = Utf8Array::<i32>::from_iter(bb);
    let q = al.get_value(2) ;

    let q :String = q.into();
    println!("{}", q)
}
