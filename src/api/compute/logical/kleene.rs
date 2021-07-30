use crate::api::compute::cast::list::Islist;
use crate::api::prelude::{ArrowError, Result};
use crate::api::List;
use crate::array::BooleanArray;
use crate::compute;

impl List {
    pub fn and(&self, rhs: &Self) -> Result<BooleanArray> {
        match self {
            Self::Bool(_value) => compute::boolean_kleene::and(_value, &rhs.as_bool().unwrap()),

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
    pub fn or(&self, rhs: &Self) -> Result<BooleanArray> {
        match self {
            Self::Bool(_value) => compute::boolean_kleene::or(_value, &rhs.as_bool().unwrap()),

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
    pub fn not(&self, rhs: &Self) -> Result<BooleanArray> {
        match self {
            Self::Bool(_value) => Ok(compute::boolean::not(_value )),

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }


}
