// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::api::scalar::DataValue;

use crate::api::columns::DataColumn;
use crate::api::data_value_operator::DataValueComparisonOperator;
use crate::array::{clone, Array, ArrayRef, Float64Array, Int32Array, Utf8Array};
use crate::datatypes::DataType;
use crate::error::*;
use crate::scalar::{Int32Scalar, Scalar, Utf8Scalar};
use std::iter::FromIterator;
use std::ops::Deref;

impl DataColumn {
    #[inline]
    pub fn data_type(&self) -> &DataType {
        match self {
            DataColumn::Array(array) => array.data_type(),
            DataColumn::Constant(scalar, _) => scalar.0.data_type(),
        }
    }
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            DataColumn::Array(array) => array.len(),
            DataColumn::Constant(_, size) => *size,
        }
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        match self {
            DataColumn::Array(array) => array.len() == 0,
            DataColumn::Constant(_, size) => *size == 0,
        }
    }
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> DataColumn {
        match self {
            DataColumn::Array(array) => array.slice(offset, length).into_data_column(),
            DataColumn::Constant(scalar, _) => DataColumn::Constant(scalar.clone(), length),
        }
    }
    pub fn get_array_ref(&self) -> Result<Arc<dyn Array>> {
        match self {
            DataColumn::Array(array) => Ok(array.clone()),
            DataColumn::Constant(scalar, size) => Ok(scalar.0.to_boxed_array(*size)),
        }
    }
}

#[test]
fn new_test() {
    let bb = vec![Some("a"), None, Some("c")];

    let al = Utf8Array::<i32>::from_iter(bb);
    let s = &al.into_data_column();
    let rr = s
        .eq(&Utf8Scalar::<i32>::new(Some("a")).into_data_column())
        .unwrap();
    let p = format!(
        "{}",
        s.filter(&rr.get_array_ref().unwrap().as_any().downcast_ref().unwrap())
            .unwrap()
            .get_array_ref()
            .unwrap()
    );
    println!("{}", p)
}
