use crate::api::columns::DataColumn;
use crate::api::columns::DataColumn::{Array, Constant};
use crate::api::comm_comparison;
use crate::api::data_type_coercion::numerical_arithmetic_coercion;
use crate::api::data_value_operator::DataValueComparisonOperator;
use crate::array::BooleanArray;
use crate::compute::comparison::Operator;
use crate::error::*;
use crate::api::prelude::{DataType, numerical_coercion};

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
    fn compare(&self, op: DataValueComparisonOperator, rhs: &DataColumn) -> Result<BooleanArray> {
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
        let (l,r) = DataColumn::coerce_cmp_lhs_rhs( self, rhs)?;
        match (&r, &l) {
            (Array(r_value), Array(l_value)) => {
                comm_comparison::comparison_array(r_value.as_ref(), op, l_value.as_ref())
            }
            (Array(r_value), Constant(l_value, _)) => {
                comm_comparison::comparison_scalar(r_value.as_ref(), op, l_value)
            }
            (Constant(r_value, _), Array(l_value)) => {
                comm_comparison::comparison_scalar(l_value.as_ref(), op, r_value)
            }
            (Constant(r_value, _), Constant(l_value, _)) => comm_comparison::comparison_array(
                l_value.0.to_boxed_array(1).as_ref(),
                op,
                r_value.0.to_boxed_array(1).as_ref(),
            ),
        }
    }
    pub fn eq(&self, rhs: &DataColumn) -> Result<BooleanArray> {
        self.compare(DataValueComparisonOperator::Eq, rhs)
    }
    pub fn neq(&self, rhs: &DataColumn) -> Result<BooleanArray> {
        self.compare(DataValueComparisonOperator::NotEq, rhs)
    }

    pub fn gt(&self, rhs: &DataColumn) -> Result<BooleanArray> {
        self.compare(DataValueComparisonOperator::Gt, rhs)
    }

    pub fn gt_eq(&self, rhs: &DataColumn) -> Result<BooleanArray> {
        self.compare(DataValueComparisonOperator::GtEq, rhs)
    }

    pub fn lt(&self, rhs: &DataColumn) -> Result<BooleanArray> {
        self.compare(DataValueComparisonOperator::Lt, rhs)
    }

    pub fn lt_eq(&self, rhs: &DataColumn) -> Result<BooleanArray> {
        self.compare(DataValueComparisonOperator::LtEq, rhs)
    }

    pub fn like(&self, rhs: &DataColumn) -> Result<BooleanArray> {
        self.likes(false, rhs)
    }

    pub fn nlike(&self, rhs: &DataColumn) -> Result<BooleanArray> {
        self.likes(true, rhs)
    }


}
