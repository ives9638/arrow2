// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::api::scalar::DataValue;

use crate::api::columns::DataColumn;
use crate::array::{Array, ArrayRef, Utf8Array, Int32Array, Float64Array};
use crate::datatypes::DataType;
use crate::scalar::Int32Scalar;
use std::iter::FromIterator;
use crate::api::data_value_operator::DataValueComparisonOperator;
use parquet2::schema::ConvertedType::Uint64;

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
}

#[test]
fn new_test() {
    let data = vec![Some(1), None, Some(10)];
    let data1 = vec![Some(1.0), None, Some(10.0)];
    let bb = vec![Some("a"), None, Some("c")];
    let a = Int32Scalar::new(DataType::Int32, Some(23));
    let b = Int32Scalar::new(DataType::Int32, Some(2));

    let al = Utf8Array::<i32>::from_iter(bb);
    let a3= Int32Array::from_iter(data);
    let a2= Float64Array::from_iter(data1);
    let q = al.get_value(2);

    let q: String = q.into();

    let c1 = a3.into_data_column();
    let cc = (&c1 + &c1).unwrap();

    let ff = c1.compare(DataValueComparisonOperator::GtEq,&a2.into_data_column()).unwrap();
    println!("{}", ff.is_empty())
}
