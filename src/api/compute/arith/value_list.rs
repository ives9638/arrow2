use crate::api::compute::cast::value::isval;
use crate::api::types::valueList::ValList;
use crate::compute::arithmetics::*;
use std::ops::Add;
impl Add for ValList {
    type Output = ValList;

    fn add(self, rhs: Self) -> Self::Output {
        match self {
            Self::I8Vec(_value) => {
                arithmetic_primitive(&_value, Operator::Add, &rhs.as_i8Vec().unwrap())
                    .unwrap()
                    .into()
            }
            Self::I16Vec(_value) => {
                arithmetic_primitive(&_value, Operator::Add, &rhs.as_i16Vec().unwrap())
                    .unwrap()
                    .into()
            }

            _ => {
                todo!()
            }
        }
    }
}
impl Add for &ValList {
    type Output = ValList;

    fn add(self, rhs: Self) -> Self::Output {
        match self {
            ValList::I8Vec(_value) => {
                arithmetic_primitive(_value, Operator::Add, &rhs.into_i8Vec().unwrap())
                    .unwrap()
                    .into()
            }
            ValList::I16Vec(_value) => {
                arithmetic_primitive(_value, Operator::Add, &rhs.into_i16Vec().unwrap())
                    .unwrap()
                    .into()
            }
            ValList::I32Vec(_value) => {
                arithmetic_primitive(_value, Operator::Add, &rhs.into_i32Vec().unwrap())
                    .unwrap()
                    .into()
            }
            _ => {
                todo!()
            }
        }
    }
}