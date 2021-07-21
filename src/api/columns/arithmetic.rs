use std::ops::Add;
use std::ops::Div;
use std::ops::Mul;
use std::ops::Rem;
use std::ops::Sub;

use crate::api::columns::DataColumn;
use crate::api::columns::DataColumn::{Array, Constant};
use crate::api::comm_arithmetic;
use crate::api::comm_arithmetic::arithmetic_type_cast;
use crate::api::data_value_operator::*;

use crate::compute::arithmetics::{can_arithmetic, Operator};
use crate::datatypes::DataType;

use crate::api::prelude::DataValueArithmeticOperator::Plus;
use crate::api::prelude::*;
impl DataColumn {
    pub fn data_type_to(lhs: &DataColumn, dtype: &DataType) -> Result<DataColumn> {
        if lhs.data_type() == dtype {
            return Ok(lhs.clone());
        }

        match lhs {
            Array(arr) => Ok(comm_arithmetic::arithmetic_type_cast(arr.as_ref(), dtype)?
                .as_ref()
                .into_data_column()),
            Constant(val, size) => Ok(arithmetic_type_cast(
                val.0.to_boxed_array(1).as_ref(),
                dtype,
            )
            .unwrap()
            .get_value(1)
            .into()),
        }
    }
    pub fn data_arithmetic_ops(
        rhs: &DataColumn,
        lhs: &DataColumn,
        op: Operator,
    ) -> Result<DataColumn> {
        if !can_arithmetic(lhs.data_type(), op, rhs.data_type()) {
            return Err(ArrowError::NotYetImplemented(format!(
                "arithmetic not supported  {:?} , {:?}  for Operator {:?}",
                lhs.data_type(),
                rhs.data_type(),
                op
            )));
        }
        match (rhs, lhs) {
            (Array(r_value), Array(l_value)) => {
                Ok(
                    comm_arithmetic::arithmetic_array(r_value.as_ref(), op, l_value.as_ref())?
                        .into_data_column(),
                )
            }
            (Array(r_value), Constant(l_value, _)) => {
                Ok(
                    comm_arithmetic::arithmetic_scalar(r_value.as_ref(), op, l_value)?
                        .into_data_column(),
                )
            }
            (Constant(r_value, _), Array(l_value)) => {
                Ok(
                    comm_arithmetic::arithmetic_scalar(l_value.as_ref(), op, r_value)?
                        .into_data_column(),
                )
            }
            (Constant(r_value, _), Constant(l_value, _)) => Ok(comm_arithmetic::arithmetic_array(
                l_value.0.to_boxed_array(1).as_ref(),
                op,
                r_value.0.to_boxed_array(1).as_ref(),
            )?
            .into_data_column()),
        }
    }
}
macro_rules! apply_arithmetic {
    ($self: ident, $rhs: ident, $op: ident, $op2: ident) => {{
        let dtype = numerical_arithmetic_coercion(
            &DataValueArithmeticOperator::$op2,
            $self.data_type(),
            $rhs.data_type(),
        )?;
        let l = DataColumn::data_type_to($self, &dtype)?;
        let r = DataColumn::data_type_to($rhs, &dtype)?;

        DataColumn::data_arithmetic_ops(&l, &r, Operator::$op)
    }};
}

impl Add for &DataColumn {
    type Output = Result<DataColumn>;
    fn add(self, rhs: Self) -> Self::Output {
        apply_arithmetic!(self, rhs, Add, Plus)
    }
}
impl Sub for &DataColumn {
    type Output = Result<DataColumn>;

    fn sub(self, rhs: Self) -> Self::Output {
        apply_arithmetic!(self, rhs, Subtract, Minus)
    }
}
impl Mul for &DataColumn {
    type Output = Result<DataColumn>;

    fn mul(self, rhs: Self) -> Self::Output {
        apply_arithmetic!(self, rhs, Multiply, Mul)
    }
}
impl Div for &DataColumn {
    type Output = Result<DataColumn>;

    fn div(self, rhs: Self) -> Self::Output {
        apply_arithmetic!(self, rhs, Divide, Div)
    }
}
impl Rem for &DataColumn {
    type Output = Result<DataColumn>;

    fn rem(self, rhs: Self) -> Self::Output {
        apply_arithmetic!(self, rhs, Remainder, Modulo)
    }
}
