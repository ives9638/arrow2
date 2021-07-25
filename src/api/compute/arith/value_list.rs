use crate::api::compute::cast::value::isval;
use crate::api::types::valueList::ValList;
use crate::compute::arithmetics::*;
use std::ops::{Add, Deref};
use crate::api::compute::cast::value_list::islist;
use crate::compute::arithmetics::basic::add::add;

impl Add for &ValList {
    type Output = Self ;

    fn add(self, rhs: Self) -> Self::Output {
        match self {
            ValList::I8(_value) => {
               add(_value, &rhs.into_i8().unwrap()).unwrap().into()
            }
            ValList::I16(_value) => {
                arithmetic_primitive(&_value, Operator::Add, &rhs.as_i16().unwrap())
                    .unwrap()
                    .into()
            }
            ValList::I32(_value) => {
                arithmetic_primitive(&_value, Operator::Add, &rhs.as_i32().unwrap())
                    .unwrap()
                    .into()
            }
            _ => {
                todo!()
            }
        }
    }
}
