use crate::scalar::{BooleanScalar, PrimitiveScalar, Scalar, Utf8Scalar};
use std::sync::Arc;

pub type I8 = PrimitiveScalar<i8>;
pub type I16 = PrimitiveScalar<i16>;
pub type I32 = PrimitiveScalar<i32>;
pub type I64 = PrimitiveScalar<i64>;
pub type IStr = Utf8Scalar<i32>;
pub type IText = Utf8Scalar<i64>;
pub type IBool = BooleanScalar;
pub type IDate32 = PrimitiveScalar<i32>;
pub type IDate64 = PrimitiveScalar<i64>;

pub struct IValue(pub Arc<dyn Scalar>);




