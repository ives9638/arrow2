mod compare;

use crate::api::prelude::{ArrowError, Result};
pub trait ArrayComare<Rhs> {
    type Output;

    fn Eq(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    fn Neq(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }

    fn Gt(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }

    fn GtEq(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    fn Lt(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    fn LtEq(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    fn like(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    fn not_like(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
}
