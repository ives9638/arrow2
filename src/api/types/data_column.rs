use crate::api::prelude::{array, Arc, ArrowError, DataType, Result};

use crate::api::types::value::Value;
use crate::api::List;
use crate::api::list::Islist;
use crate::api::prelude::array::Array;
use std::borrow::Borrow;

#[derive(Clone, Debug)]
pub enum DataColumn {
    // Array of values.
    Array(List),
    // A Single value.
    Constant(Value, usize),
}

impl DataColumn {
    #[inline]
    pub fn data_type(&self) -> DataType {
        match self {
            DataColumn::Array(array) => array.data_type().clone(),
            DataColumn::Constant(v, _) => v.data_type() ,
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
            DataColumn::Array(array) => DataColumn::Array(array.slices(offset, length).unwrap()),
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
impl DataColumn {
    #[inline]
    pub fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        let size = self.len();
        let (col, row) = match self {
            DataColumn::Array(array) => (array.clone(), None),
            DataColumn::Constant(v, _) => (v.into_list(1), Some(0_usize)),
        };

        match col.data_type() {
            DataType::Boolean => {
                let array = col.as_bool().unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&[array.value(row.unwrap_or(i)) as u8]);
                }
            }
            DataType::Float32 => {
                let array = col.as_f32().unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Float64 => {
                let array = col.as_f64() .unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt8 => {
                let array = col.as_u8().unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt16 => {
                let array = col.as_u16().unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt32 => {
                let array = col.as_u32().unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt64 => {
                let array = col.as_u64().unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int8 => {
                let array = col.as_i8().unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int16 => {
                let array = col.as_i16().unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int32 => {
                let array = col.as_i32().unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int64 => {
                let array = col.as_i64().unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Utf8 => {
                let array = col.as_str().unwrap();

                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    let value = array.value(row.unwrap_or(i));
                    // store the size
                    v.extend_from_slice(&value.len().to_le_bytes());
                    // store the string value
                    v.extend_from_slice(value.as_bytes());
                }
            }
            DataType::Date32 => {
                let array = col.as_i32().unwrap();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }

            _ => {
                // This is internal because we should have caught this before.
                return Result::Err(ArrowError::NotYetImplemented(format!(
                    "Unsupported the col type creating key {}",
                    col.data_type()
                )));
            }
        }
        Ok(())
    }
}
impl From<List> for DataColumn {
    fn from(array: List) -> Self {
        DataColumn::Array(array)
    }
}
