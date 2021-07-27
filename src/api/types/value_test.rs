
use crate::api::prelude::arithmetics::ArrayAdd;
use crate::api::prelude::array::{ Int32Array, Int8Array};

use crate::api::types::list::List;
use crate::api::compute::ArrayComare;
use crate::compute::cast;
use crate::api::compute::cast::list::Islist;
use itertools::Format;
use std::fmt::Display;
use crate::api::compute::cast::value::Isval;

#[test]
fn test_data_block() -> Result<(), ()> {
    let data = vec![Some(8i8), None, Some(9)];
    let data2 = vec![Some(82), Some(182), Some(19)];


    let array: Int8Array = data.into_iter().collect();
    let array2: Int32Array = data2.into_iter().collect();

    let v = List::from(array);

    let v2 = List::from(array2);

    let sum =  v.sum().unwrap();


    let time= cast::utf8_to_timestamp_ns_scalar("2020-09-08 13:42:00" ).unwrap();


     println!("{}",format!("{}", sum.as_i8().unwrap()));
    let max =  v2.as_str();

    println!("{}",format!("{}", max.unwrap()));
    let add = v2.add(&v);
    Ok(())
}
