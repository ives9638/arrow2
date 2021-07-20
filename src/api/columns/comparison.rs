use crate::api::columns::DataColumn;
use crate::api::columns::DataColumn::{Array, Constant};
use crate::api::comm_comparison;
use crate::api::data_type_coercion::numerical_arithmetic_coercion;
use crate::api::data_value_operator::DataValueComparisonOperator;
use crate::compute::comparison::Operator;
use crate::error::*;

impl DataColumn {
    #[allow(unused)]
    pub fn compare(&self, op: DataValueComparisonOperator, rhs: &DataColumn) -> Result<DataColumn> {
        let op = match op {
            DataValueComparisonOperator::Eq => Operator::Eq,
            DataValueComparisonOperator::Lt => Operator::Lt,
            DataValueComparisonOperator::LtEq => Operator::LtEq,
            DataValueComparisonOperator::Gt => Operator::Gt,
            DataValueComparisonOperator::GtEq => Operator::GtEq,
            DataValueComparisonOperator::NotEq => Operator::Neq,
            DataValueComparisonOperator::Eq => Operator::Eq,
            DataValueComparisonOperator::Like => Operator::LtEq,
            DataValueComparisonOperator::NotLike => Operator::LtEq,
        };
        use crate::api::prelude::DataValueArithmeticOperator::Plus;
        let dtype = numerical_arithmetic_coercion(&Plus, self.data_type(), rhs.data_type())?;
        let l = DataColumn::data_type_to(self, &dtype)?;
        let r = DataColumn::data_type_to(rhs, &dtype)?;
        match (&r, &l) {
            (Array(r_value), Array(l_value)) => {
                Ok(
                    comm_comparison::comparison_array(r_value.as_ref(), op, l_value.as_ref())?
                        .into_data_column(),
                )
            }
            (Array(r_value), Constant(l_value, _)) => {
                Ok(
                    comm_comparison::comparison_scalar(r_value.as_ref(), op, l_value)?
                        .into_data_column(),
                )
            }
            (Constant(r_value, _), Array(l_value)) => {
                Ok(
                    comm_comparison::comparison_scalar(l_value.as_ref(), op, r_value)?
                        .into_data_column(),
                )
            }
            (Constant(r_value, _), Constant(l_value, _)) => Ok(comm_comparison::comparison_array(
                l_value.0.to_boxed_array(1).as_ref(),
                op,
                r_value.0.to_boxed_array(1).as_ref(),
            )?
            .into_data_column()),
        }
    }
}
