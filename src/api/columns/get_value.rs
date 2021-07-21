use crate::api::columns::DataColumn;
use crate::api::columns::DataColumn::{Array, Constant};
use crate::api::Ivalue::Ivalue;
use crate::array::{Int32Array, PrimitiveArray};
use crate::compute::take;
use crate::api::prelude::*;
impl DataColumn {
    fn get_value(&self, i: usize) -> Ivalue {
        match self {
            Array(array) => array.get_value(i).clone(),
            Constant(value, _) => value.clone(),
        }
    }
    fn take_values(&self, indices: Vec<i32>) -> Result<DataColumn> {
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

}
