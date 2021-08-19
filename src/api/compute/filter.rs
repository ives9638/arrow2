use crate::api::prelude::filter::Filter;
use crate::api::prelude::{ Result};
use crate::api::List;
use crate::compute::filter::filter;
use crate::api::prelude::array::BooleanArray;

impl List {
    pub fn filter(&self, predicate: &BooleanArray) -> Result<List> {
        Ok(filter(self, predicate)?.into())
    }
}
