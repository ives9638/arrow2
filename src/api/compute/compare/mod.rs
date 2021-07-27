#![allow(unused_variables, dead_code, missing_docs)]
mod compare;

use crate::api::prelude::{ArrowError, Result};
pub trait ArrayComare<Rhs> {
    type Output;
    #[inline]
    fn eq(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    #[inline]
    fn n_eq(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    #[inline]
    fn gt(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    #[inline]
    fn gt_eq(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    #[inline]
    fn lt(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    #[inline]
    fn lt_eq(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    #[inline]
    fn like(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
    #[inline]
    fn not_like(&self, rhs: &Rhs) -> Result<Self::Output> {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type {} does not support  logical Oper ",
            std::any::type_name::<Rhs>()
        )));
    }
}
