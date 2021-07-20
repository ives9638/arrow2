// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

mod arithmetic;
mod data_column;
mod comparison;
mod get_value;

pub use data_column::*;



use std::ops::Deref;
use crate::array::ArrayRef;
use crate::api::IValue::IValue;

#[derive(Clone)]
pub enum DataColumn {
    // Array of values.
    Array(ArrayRef),
    // A Single value.
    Constant(IValue, usize),
}
impl Into<DataColumn> for IValue {
    fn into(self) -> DataColumn {
        DataColumn::Constant( self.clone(),1usize)
    }
}
