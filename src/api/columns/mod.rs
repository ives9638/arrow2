use std::ops::Deref;

pub use data_column::*;

use crate::api::scalar::DataValue;
use crate::array::{ArrayRef, PrimitiveArray};
use crate::api::prelude::Arc;
use crate::datatypes::Schema;
use crate::types::{NativeType, NaturalDataType};
use crate::trusted_len::TrustedLen;
use crate::api::prelude::array::Array;

mod arithmetic;
mod data_column;
mod comparison;
mod base;
mod string_operator;
mod aggregate;
mod kleenelogic;


#[derive(Clone)]
pub enum DataColumn {
    // Array of values.
    Array(ArrayRef),
    // A Single value.
    Constant(DataValue, usize),
}
impl Into<DataColumn> for DataValue {
    fn into(self) -> DataColumn {
        DataColumn::Constant( self.clone(),1usize)
    }
}

impl Into<DataColumn> for &DataValue {
    fn into(self) -> DataColumn {
        DataColumn::Constant( self.clone(),1usize)
    }
}


