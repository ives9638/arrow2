use crate::api::compute::cast::list::Islist;
use crate::api::compute::cast::value::Isval;
use crate::api::prelude::array::BooleanArray;
use crate::api::prelude::{ArrowError, Result};
use crate::api::types::list::List;
use crate::api::types::value::Value;
use crate::compute::regex_match;

pub trait Regex<RHS> {
    type Output;

    fn regex(&self, rhs: &RHS) -> Result<Self::Output>;
}
impl Regex<List> for List {
    type Output = BooleanArray;

    fn regex(&self, rhs: &List) -> Result<Self::Output> {
        match self {
            Self::String(_value) => {
                regex_match::regex_match(_value, rhs.as_str().unwrap().as_ref())
            }
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
}

impl Regex<Value> for List {
    type Output = BooleanArray;

    fn regex(&self, rhs: &Value) -> Result<Self::Output> {
        match self {
            Self::String(_value) => {
                regex_match::regex_match_scalar(_value, rhs.as_str().unwrap().as_str())
            }
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
}
