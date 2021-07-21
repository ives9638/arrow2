use std::sync::Arc;
use crate::scalar::Scalar;

mod data_value_coercion;
#[derive(Clone)]
pub struct DataValue(pub Arc<dyn Scalar>);