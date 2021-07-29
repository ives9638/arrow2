use crate::api::prelude::arithmetics::ArrayAdd;
use crate::api::prelude::array::{Int32Array, Int8Array, Array};

use crate::api::compute::cast::list::Islist;
use crate::api::compute::cast::value::Isval;
use crate::api::compute::ArrayComare;
use crate::api::types::list::List;
use crate::compute::cast;
use crate::datatypes::DataType;
use itertools::Format;
use std::fmt::Display;
use crate::buffer::bytes::Bytes;
use byteorder::{BigEndian, ReadBytesExt};
use std::convert::TryInto;
use crate::array::BinaryArray;

#[test]
fn test_data_block() -> Result<(), ()> {
    let data = vec![Some(8i8), None, Some(9)];
    let data2 = vec![Some(8111112), Some(111111182), Some(111111219)];

    let s = List::new_empty(DataType::LargeUtf8);
    let s = s.get_array_ref();

    let array: Int8Array = data.into_iter().collect();
    let array2: Int32Array = data2.into_iter().collect();

    let v = List::from(array2);


    let binArray = List::pack_to_uk(&v,0u128,0);

    let mut packed_value = 0u128;
    let aa = 32i32;
    let bb = 64i64;
    let c =1i8;
    packed_value =  packed_value | (aa as u128) << (0);
    let ts = packed_value.to_le_bytes();
    let t = ((bb as u128) << (32)).to_le_bytes();

    packed_value =  packed_value | (bb as u128) << (32);

    let ts = packed_value.to_le_bytes();

    packed_value =  packed_value | (c as u128) << (64 + 32);
    let ts = packed_value.to_le_bytes();



    Ok(())
}
