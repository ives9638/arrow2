use crate::api::columns::DataColumn;
use crate::api::columns::DataColumn::{Array, Constant};
use crate::api::prelude::*;
use crate::api::scalar::DataValue;
use crate::array::{Array as ArrowArray, BooleanArray, Int32Array, PrimitiveArray, UInt64Array};
use crate::compute;
use crate::compute::take;

impl DataColumn {
    pub fn get_value(&self, i: usize) -> DataValue {
        match self {
            Array(array) => array.get_value(i).unwrap().clone(),
            Constant(value, _) => value.clone(),
        }
    }
    pub fn vec_hash(&self) -> Result<UInt64Array> {
        compute::hash::hash(self.get_array_ref().unwrap().as_ref())
    }
    pub fn take(&self, indices: Vec<i32>) -> Result<DataColumn> {
        match self {
            Array(array) => {
                let indic = PrimitiveArray::from_trusted_len_values_iter(indices.into_iter());
                Ok(take::take(array.as_ref(), &indic)?
                    .as_ref()
                    .into_data_column())
            }
            Constant(value, _) => Ok(value.into()),
        }
    }
    pub fn concat(columns: &[DataColumn]) -> Result<DataColumn> {
        let arrays = columns
            .iter()
            .map(|s| s.get_array_ref())
            .collect::<Result<Vec<_>>>()?;
        let dyn_arrays: Vec<&dyn ArrowArray> = arrays.iter().map(|arr| arr.as_ref()).collect();
        Ok(compute::concat::concatenate(dyn_arrays.as_slice())?.into_data_column())
    }
    pub fn contains(&self, rhs: &DataColumn) -> Result<BooleanArray> {
        compute::contains::contains(
            self.get_array_ref().unwrap().as_ref(),
            rhs.get_array_ref().unwrap().as_ref(),
        )
    }
    pub fn filter(&self, filter: &BooleanArray) -> Result<DataColumn> {
        Ok(
            compute::filter::filter(self.get_array_ref().unwrap().as_ref(), filter)?
                .as_ref()
                .into_data_column(),
        )
    }

}
