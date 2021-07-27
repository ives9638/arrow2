#![allow(dead_code)]
use crate::api::prelude::substring::{utf8_substring};
use crate::api::prelude::{ArrowError, Result};
use crate::api::types::list::List;

impl List {
    pub fn sub_string(&self, start: i64, length: &Option<i64>) -> Result<Self> {
        match self {
            Self::String(_value) => {
                Ok(utf8_substring(_value, start as i32, &length.map(|e| e as i32)).into())
            }
            Self::Text(_value) => Ok(utf8_substring(_value, start, length).into()),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
}

