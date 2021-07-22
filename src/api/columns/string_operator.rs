use crate::api::columns::DataColumn::{Array, Constant};
use crate::api::prelude::{ArrowError, DataColumn, DataType, DataValueComparisonOperator, Result};
use crate::api::{comm_arithmetic, comm_comparison};
use crate::array::BooleanArray;
use crate::compute::substring::substring;

impl DataColumn {
    #[allow(unused)]
    pub(crate) fn likes(&self, not: bool, rhs: &DataColumn) -> Result<BooleanArray> {
        if self.data_type() != rhs.data_type() || self.data_type() != &DataType::Utf8 {
            return Err(ArrowError::NotYetImplemented(format!(
                "like not supported  {:?} , {:?}  for Operator like",
                self.data_type(),
                rhs.data_type()
            )));
        }
        let op: DataValueComparisonOperator = if not {
            DataValueComparisonOperator::NotLike
        } else {
            DataValueComparisonOperator::Like
        };
        match (self, rhs) {
            (Array(r_value), Array(l_value)) => {
                comm_comparison::comparison_udf8_array(r_value.as_ref(), op, l_value.as_ref())
            }
            (Array(r), Constant(l, _)) => {
                comm_comparison::comparison_udf8_scalar(r.as_ref(), op, l)
            }
            (Constant(r_value, _), Array(l_value)) => {
                comm_comparison::comparison_udf8_scalar(l_value.as_ref(), op, r_value)
            }
            (Constant(r_value, _), Constant(l_value, _)) => comm_comparison::comparison_udf8_array(
                l_value.0.to_boxed_array(1).as_ref(),
                op,
                r_value.0.to_boxed_array(1).as_ref(),
            ),
        }
    }
    pub fn sub_str(&self, start: i64, length: &Option<u64>) -> Result<DataColumn> {
        match (self) {
            Array(r_value) => Ok(substring(r_value.as_ref(), start, length)?.into_data_column()),
            Constant(l, _) => {
                Ok(substring(l.0.to_boxed_array(1).as_ref(), start, length)?.into_data_column())
            }
        }
    }
    pub fn regex(&self, regx: &DataColumn) -> Result<BooleanArray> {
        match (self, regx) {
            (Array(r_value), Array(l_value)) => {
                comm_comparison::comparison_udf_regex_array(r_value.as_ref(), l_value.as_ref())
            }
            (Array(r), Constant(l, _)) => {
                comm_comparison::comparison_udf_regex_scalar(r.as_ref(), l)
            }
            (Constant(r_value, _), Array(l_value)) => {
                comm_comparison::comparison_udf_regex_scalar(l_value.as_ref(), r_value)
            }
            (Constant(r_value, _), Constant(l_value, _)) => {
                comm_comparison::comparison_udf_regex_array(
                    l_value.0.to_boxed_array(1).as_ref(),
                    r_value.0.to_boxed_array(1).as_ref(),
                )
            }
        }
    }
}
