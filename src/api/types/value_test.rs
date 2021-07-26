use crate::api::compute::cast::list::Islist;
use crate::api::prelude::arithmetics::ArrayAdd;
use crate::api::prelude::array::{BooleanArray, Int32Array, PrimitiveArray, Int8Array};
use crate::api::prelude::{array, Arc, DataType};
use crate::api::types::list::List;
use crate::api::types::value::Value;
use crate::array::Array;
use crate::compute::arithmetics::basic::add::add;
use crate::compute::arithmetics::{arithmetic, Operator};
use std::ops::Add;
use crate::api::compute::ArrayComare;

#[test]
fn test_data_block() -> Result<(), ()> {
    let data = vec![Some(8i8), None, Some(9)];
    let data2 = vec![Some(82), Some(182), Some(19)];


    let array: Int8Array = data.into_iter().collect();
    let array2: Int32Array = data2.into_iter().collect();

    let v = List::from(array);

    let v2 = List::from(array2);

    let sum =  v.sum().unwrap();
    let max = v.max().unwrap();
    let bol = v2.Lt(&v) ;
    let add = v2.add(&v);
    Ok(())
}
