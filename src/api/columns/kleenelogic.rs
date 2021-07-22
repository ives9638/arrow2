use crate::api::prelude::{ArrowError, DataColumn, Result};
use crate::array::BooleanArray;
use crate::compute;
impl DataColumn {
    pub fn and(&self, rhs: &DataColumn) -> Result<BooleanArray> {
        compute::boolean_kleene::and(
            self.get_array_ref()
                .unwrap().as_ref()
                .as_any()
                .downcast_ref()
                .unwrap(),
            rhs.get_array_ref()
                .unwrap().as_ref()
                .as_any()
                .downcast_ref()
                .unwrap(),
        )
    }
    pub fn or(&self, rhs: &DataColumn) -> Result<BooleanArray> {
        compute::boolean_kleene::or(
            self.get_array_ref()
                .unwrap().as_ref()
                .as_any()
                .downcast_ref()
                .unwrap(),
            rhs.get_array_ref()
                .unwrap().as_ref()
                .as_any()
                .downcast_ref()
                .unwrap(),
        )
    }
    pub fn not(&self) -> Result<BooleanArray> {
        Ok(compute::boolean::not(
            self.get_array_ref()
                .unwrap().as_ref()
                .as_any()
                .downcast_ref()
                .unwrap(),
        ))
    }
    pub fn is_null(&self) -> Result<BooleanArray> {
        Ok(compute::boolean::is_null(
            self.get_array_ref()
                .unwrap().as_ref()
        ))
    }
    pub fn is_not_null(&self) -> Result<BooleanArray> {
        Ok(compute::boolean::is_not_null(
            self.get_array_ref()
                .unwrap().as_ref()
        ))
    }
}
