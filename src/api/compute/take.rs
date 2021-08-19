use crate::api::prelude::array::{Array, PrimitiveArray};
use crate::api::prelude::take::{boolean, primitive, utf8};
use crate::api::prelude::{take, Index};
use crate::api::prelude::{ArrowError, Result};
use crate::api::List;
use itertools::Itertools;
use std::borrow::Borrow;
use std::collections::HashMap;

impl List {
    pub fn take<T>(&self, idx: &PrimitiveArray<T>) -> Result<Self>
    where
        T: Index,
    {
        match self {
            Self::I8(_value) => Ok(primitive::take(_value, idx).into()),
            Self::I16(_value) => Ok(primitive::take(_value, idx).into()),
            Self::I32(_value) => Ok(primitive::take(_value, idx).into()),
            Self::I64(_value) => Ok(primitive::take(_value, idx).into()),

            Self::U8(_value) => Ok(primitive::take(_value, idx).into()),
            Self::U16(_value) => Ok(primitive::take(_value, idx).into()),
            Self::U32(_value) => Ok(primitive::take(_value, idx).into()),
            Self::U64(_value) => Ok(primitive::take(_value, idx).into()),

            Self::String(_value) => Ok(utf8::take(_value, idx).into()),
            Self::Text(_value) => Ok(utf8::take(_value, idx).into()),
            Self::Bool(_value) => Ok(boolean::take(_value, idx).into()),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<List>(),
                self.data_type()
            ))),
        }
    }
    pub fn scatter_unchecked(
        &self,
        indices: &mut dyn Iterator<Item = u64>,
        scattered_size: usize,
    ) -> Result<Vec<Self>> {
        let indices = indices.zip((0..self.len() as u64)).into_group_map_by(|p| p.0);

        let lookup: HashMap<&u64, Vec<u64>> = indices
            .iter()
            .map(|p| (p.0, p.1.iter().map(|x| x.1 as u64).collect_vec()))
            .collect();

        let ret: Vec<Self> = lookup
            .iter()
            .map(|kv| {
                let idx = PrimitiveArray::<u64>::from_slice(kv.1.as_slice());
                self.take(&idx).unwrap()
            })
            .collect();
        Ok(ret)
    }
}
