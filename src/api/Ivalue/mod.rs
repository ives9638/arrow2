use std::sync::Arc;
use crate::scalar::Scalar;

mod Ivalue_convert;
#[derive(Clone)]
pub struct Ivalue(pub Arc<dyn Scalar>);