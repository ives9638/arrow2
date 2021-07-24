use crate::api::prelude::{Arc, DataType, array};
use crate::api::types::value::Value;
use crate::api::prelude::array::PrimitiveArray;
use crate::compute::arithmetics::basic::add::add;
use crate::compute::arithmetics::{arithmetic, Operator};
use crate::array::Array;
use crate::api::types::valueList::ValList;

#[test]
fn test_data_block() -> Result<(), ()> {
    let v = ValList::from_vec(vec![1i16, 2, 3].into_iter());
    let vv = ValList::from_vec(vec![1i32, 2, 3].into_iter());
    let c = &vv + &v ;
    let c =c.as_f64Vec().unwrap().value(0);
    Ok(())
}
