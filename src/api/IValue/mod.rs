use std::sync::Arc;
use crate::scalar::Scalar;

pub(crate) mod types;
mod IValue_convert;
pub struct IValue(pub Arc<dyn Scalar>);