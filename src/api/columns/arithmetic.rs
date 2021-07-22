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

use crate::compute::arithmetics::{can_arithmetic, negate, Operator};
use crate::datatypes::DataType;

use crate::api::prelude::DataValueArithmeticOperator::Plus;
use crate::api::prelude::*;
use crate::array::{
    Array as ArrowArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
};

impl DataColumn {
    pub fn negative(&self) -> Result<DataColumn> {
        match self.data_type() {
            DataType::Int8 => Ok(negate(
                self.get_array_ref()?
                    .as_ref()
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap(),
            )
            .into_data_column()),
            DataType::Int16 => Ok(negate(
                self.get_array_ref()?
                    .as_ref()
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .unwrap(),
            )
            .into_data_column()),
            DataType::Int32 => Ok(negate(
                self.get_array_ref()?
                    .as_ref()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap(),
            )
            .into_data_column()),
            DataType::Int64 => Ok(negate(
                self.get_array_ref()?
                    .as_ref()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
            )
            .into_data_column()),
            DataType::Float32 => Ok(negate(
                self.get_array_ref()?
                    .as_ref()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap(),
            )
            .into_data_column()),
            DataType::Float64 => Ok(negate(
                self.get_array_ref()?
                    .as_ref()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap(),
            )
            .into_data_column()),

            _ => Err(ArrowError::InvalidArgumentError(format!(
                "DataType {:?} is Unsupported for neg op",
                self.data_type()
            ))),
        }
    }
    pub fn cast_to(&self, dtype: &DataType) -> Result<DataColumn> {
        if self.data_type() == dtype {
            return Ok(self.clone());
        }

        match self {
            Array(arr) => Ok(comm_arithmetic::arithmetic_type_cast(arr.as_ref(), dtype)?
                .as_ref()
                .into_data_column()),
            Constant(val, size) => Ok(arithmetic_type_cast(
                val.0.to_boxed_array(1).as_ref(),
                dtype,
            )
            .unwrap()
            .get_value(1)
            .unwrap()
            .into()),
        }
    }
    fn data_arithmetic_ops(rhs: &DataColumn, lhs: &DataColumn, op: Operator) -> Result<DataColumn> {
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
        let l = $self.cast_to(&dtype)?;
        let r = $rhs.cast_to(&dtype)?;

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
