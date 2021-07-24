use crate::api::columns::DataColumn;
use crate::api::columns::DataColumn::{Array, Constant};
use crate::api::comm_comparison;
use crate::api::data_type_coercion::numerical_arithmetic_coercion;
use crate::api::data_value_operator::DataValueComparisonOperator;
use crate::api::prelude::{numerical_coercion, DataType};
use crate::array::{Array as ArrowArray, BooleanArray};
use crate::compute::comparison::Operator;
use crate::error::*;

impl DataColumn {
    fn coerce_cmp_lhs_rhs(lhs: &DataColumn, rhs: &DataColumn) -> Result<(DataColumn, DataColumn)> {
        if lhs.data_type() == rhs.data_type()
            && (lhs.data_type() == &DataType::Utf8 || lhs.data_type() == &DataType::Boolean)
        {
            return Ok((lhs.clone(), rhs.clone()));
        }

        let dtype = numerical_coercion(&lhs.data_type(), &rhs.data_type())?;

        let mut left = lhs.clone();
        if lhs.data_type() != &dtype {
            left = lhs.cast_to(&dtype)?;
        }

        let mut right = rhs.clone();
        if rhs.data_type() != &dtype {
            right = rhs.cast_to(&dtype)?;
        }

        Ok((left, right))
    }
    #[allow(unused)]
    fn compare(&self, op: DataValueComparisonOperator, rhs: &DataColumn) -> Result<DataColumn> {
        let op = match op {
            DataValueComparisonOperator::Eq => Operator::Eq,
            DataValueComparisonOperator::Lt => Operator::Lt,
            DataValueComparisonOperator::LtEq => Operator::LtEq,
            DataValueComparisonOperator::Gt => Operator::Gt,
            DataValueComparisonOperator::GtEq => Operator::GtEq,
            DataValueComparisonOperator::NotEq => Operator::Neq,
            _ => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "compare  {:?} is not supported",
                    op
                )));
            }
        };
        let (l, r) = DataColumn::coerce_cmp_lhs_rhs(self, rhs)?;
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
    pub fn eq(&self, rhs: &DataColumn) -> Result<DataColumn> {
        self.compare(DataValueComparisonOperator::Eq, rhs)
    }
    pub fn neq(&self, rhs: &DataColumn) -> Result<DataColumn> {
        self.compare(DataValueComparisonOperator::NotEq, rhs)
    }

    pub fn gt(&self, rhs: &DataColumn) -> Result<DataColumn> {
        self.compare(DataValueComparisonOperator::Gt, rhs)
    }

    pub fn gt_eq(&self, rhs: &DataColumn) -> Result<DataColumn> {
        self.compare(DataValueComparisonOperator::GtEq, rhs)
    }

    pub fn lt(&self, rhs: &DataColumn) -> Result<DataColumn> {
        self.compare(DataValueComparisonOperator::Lt, rhs)
    }

    pub fn lt_eq(&self, rhs: &DataColumn) -> Result<DataColumn> {
        self.compare(DataValueComparisonOperator::LtEq, rhs)
    }

    pub fn like(&self, rhs: &DataColumn) -> Result<DataColumn> {
        self.likes(false, rhs)
    }

    pub fn nlike(&self, rhs: &DataColumn) -> Result<DataColumn> {
        self.likes(true, rhs)
    }
}
