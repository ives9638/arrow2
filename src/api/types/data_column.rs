use crate::api::prelude::{array, Arc, ArrowError, DataType, Result};

use crate::api::types::value::Value;
use crate::api::List;

#[derive(Clone, Debug)]
pub enum DataColumn {
    // Array of values.
    Array(List),
    // A Single value.
    Constant(Value, usize),
}

impl DataColumn {
    #[inline]
    pub fn data_type(&self) -> &DataType {
        match self {
            DataColumn::Array(array) => array.data_type(),
            DataColumn::Constant(v, _) => v.data_type(),
        }
    }

    #[inline]
    pub fn to_array(&self) -> Result<List> {
        match self {
            DataColumn::Array(array) => Ok(array.clone()),
            DataColumn::Constant(scalar, size) => Ok(scalar.into_list(*size)),
        }
    }

    /// Return the minimal series, if it's constant value, it's size is 1.
    /// This could be useful when Constant <op> Constant
    /// Since our kernel is based on Array <op> Array
    /// 1. Constant -----> minimal Array; 2. Array <op> Array; 3. resize_constant
    #[inline]
    pub fn to_minimal_array(&self) -> Result<List> {
        match self {
            DataColumn::Array(array) => Ok(array.clone()),
            DataColumn::Constant(scalar, _) => Ok(scalar.into_list(1)),
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
            DataColumn::Array(array) => DataColumn::Array(array.slice(offset, length).unwrap()),
            DataColumn::Constant(scalar, _) => DataColumn::Constant(scalar.clone(), length),
        }
    }
    #[inline]
    pub fn get_array_memory_size(&self) -> usize {
        match self {
            DataColumn::Array(array) => array.get_array_memory_size(),
            DataColumn::Constant(scalar, size) => {
                scalar.into_list(*size).get_array_memory_size()
            }
        }
    }

    #[inline]
    pub fn resize_constant(&self, size: usize) -> Self {
        match self {
            DataColumn::Array(array) if array.len() == 1 => {
                let value = array.get_value(0).unwrap();
                DataColumn::Constant(value, size)
            }
            DataColumn::Constant(scalar, _) => DataColumn::Constant(scalar.clone(), size),
            _ => self.clone(),
        }
    }
}
impl From<List> for DataColumn {
    fn from(array: List) -> Self {
        DataColumn::Array(array)
    }
}
