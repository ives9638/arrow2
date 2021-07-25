use crate::api::compute::cast::list::islist;
use crate::api::prelude::arithmetics::ArrayAdd;
use crate::api::prelude::array::{PrimitiveArray, BooleanArray, Int32Array};
use crate::api::prelude::{array, Arc, DataType};
use crate::api::types::list::List;
use crate::api::types::value::Value;
use crate::array::Array;
use crate::compute::arithmetics::basic::add::add;
use crate::compute::arithmetics::{arithmetic, Operator};
use std::ops::Add;

#[test]
fn test_data_block() -> Result<(), ()> {
    let data = vec![Some(8), None, Some(9)];
    let array: Int32Array = data.into_iter().collect();
    let vv =List::from(array);

    let bol = vv.as_i32().unwrap();
    let g = bol.add(bol.as_ref()).unwrap();

    Ok(())
}
