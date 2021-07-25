use crate::api::prelude::{Arc, DataType, array};
use crate::api::types::value::Value;
use crate::api::prelude::array::PrimitiveArray;
use crate::compute::arithmetics::basic::add::add;
use crate::compute::arithmetics::{arithmetic, Operator};
use crate::array::Array;
use crate::api::types::valueList::ValList;
use std::ops::Add;
use crate::api::prelude::arithmetics::ArrayAdd;
use crate::api::compute::cast::value_list::islist;

#[test]
fn test_data_block() -> Result<(), ()> {
    let v = ValList::from_vec(vec![1i8, 2, 3].into_iter());

    let vv = ValList::from_vec(vec![1i8, 2, 3].into_iter());
    //let c =   &v.as_i8().unwrap().add(  vv.as_i8().unwrap().as_ref()).unwrap() ;
    let c =   v.as_ref().add( vv.as_ref() );

    let dd =c.data_type();
    Ok(())
}
