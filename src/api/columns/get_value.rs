use crate::api::columns::DataColumn;
use crate::api::columns::DataColumn::{Array, Constant};
use crate::api::IValue::IValue;

impl DataColumn {
    fn get_value(&self, i: usize) -> IValue {
        match self {
            Array(array) => array.get_value(i).clone(),
            Constant(value, _) => value.clone(),
        }
    }

}
