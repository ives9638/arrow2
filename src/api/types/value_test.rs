use crate::api::prelude::*;



use crate::compute::cast;
use crate::datatypes::DataType;
use itertools::{Format, Itertools};
use std::fmt::Display;
use crate::buffer::bytes::Bytes;
use byteorder::{BigEndian, ReadBytesExt};
use std::convert::TryInto;
use crate::array::BinaryArray;
use std::collections::HashMap;
use crate::api::value::Isval;

#[test]
fn test_data_block()  {
    let data = vec![Some(8i8), None, Some(9)];
    let data2 = vec![Some(8111112), Some(111111182), Some(111111219)];

    let s = List::new_empty(DataType::LargeUtf8);
    let s = s.get_array_ref();

    let array: Int8Array = data.into_iter().collect();
    let array2: Int32Array = data2.into_iter().collect();

    let v = List::from(array2);

    let binArray = List::pack_to_u128(&v,vec![0u128;3],0);
    let binArray = List::pack_to_u128(&v,binArray,32);
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

    let lookup: HashMap<&i32,Vec<i32>>  = [1,2,2,2,1].iter().zip(9..19).into_group_map_by( |x| x.0).into_iter().map(|x|{
        (x.0, x.1.iter().map(|n| n.1).collect_vec())
    }).collect();

 let d  = 21_u8 / 21_u32 ;
    let data2 = vec![Some(8111112), Some(111111182), Some(111111219),Some(1),Some(3),Some(4)];
    let indices = vec![1, 2, 3, 1, 3, 2,];
    let f  =List::from_primitive_array(data2.as_slice()).scatter_unchecked(  &mut indices.into_iter(),3 );


    let b = BooleanArray::from_slice(vec![true, false, false,false,false, true]);
    let gg  =List::from_primitive_array(data2.as_slice()).filter(&b).unwrap();

    let z = gg.get_value(0).unwrap().as_i32().unwrap();

    let z = gg.get_value(1).unwrap().as_i32().unwrap();
    let z = gg.len();
}
