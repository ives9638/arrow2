use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use crate::pretty_format_blocks;

use arrow2::api::empty_array;
use arrow2::api::prelude::{
    array,   DataColumn, DataSchemaRef, RecordBatch, Schema,
};
use common_exception::*;

#[derive(Clone)]
pub struct DataBlock {
    schema: DataSchemaRef,
    columns: Vec<DataColumn>,
}

impl DataBlock {
    pub fn create(schema: DataSchemaRef, columns: Vec<DataColumn>) -> Self {
        DataBlock { schema, columns }
    }

    pub fn create_by_array(schema: DataSchemaRef, arrays: Vec<DataColumn>) -> Self {
        DataBlock {
            schema,
            columns: arrays,
        }
    }

    pub fn empty() -> Self {
        DataBlock {
            schema: Arc::new(Schema::empty()),
            columns: vec![],
        }
    }

    pub fn empty_with_schema(schema: DataSchemaRef) -> Self {
        let mut columns = vec![];

        for f in schema.fields().iter() {
            columns.push(DataColumn::Array(empty_array(f.data_type().clone())))
        }
        DataBlock { schema, columns }
    }

    pub fn is_empty(&self) -> bool {
        self.num_columns() == 0 || self.num_rows() == 0
    }

    pub fn schema(&self) -> &DataSchemaRef {
        &self.schema
    }

    pub fn num_rows(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].len()
        }
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Data Block physical memory size
    pub fn memory_size(&self) -> usize {
        self.columns.iter().map(|x| x.get_array_memory_size()).sum()
    }

    pub fn column(&self, index: usize) -> &DataColumn {
        &self.columns[index]
    }

    pub fn columns(&self) -> &[DataColumn] {
        &self.columns
    }

    pub fn try_column_by_name(&self, name: &str) -> Result<&DataColumn> {
        if name == "*" {
            Ok(&self.columns[0])
        } else {
            let idx = self.schema.index_of(name)?;
            Ok(&self.columns[idx])
        }
    }

    pub fn column_by_name(&self, name: &str) -> Option<&DataColumn> {
        if self.is_empty() {
            return None;
        }

        if name == "*" {
            return Some(&self.columns[0]);
        };

        if let Ok(idx) = self.schema.index_of(name) {
            Some(&self.columns[idx])
        } else {
            None
        }
    }

    pub fn try_array_by_name(&self, name: &str) -> Result<&DataColumn> {
        if name == "*" {
            Ok(&self.columns[0])
        } else {
            let idx = self.schema.index_of(name)?;
            Ok(&self.columns[idx])
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let rows = self.num_rows();
        if offset == 0 && length >= rows {
            return self.clone();
        }
        let mut limited_columns = Vec::with_capacity(self.num_columns());
        for i in 0..self.num_columns() {
            limited_columns.push(self.column(i).slice(offset, length));
        }
        DataBlock::create(self.schema().clone(), limited_columns)
    }
}



impl TryFrom<RecordBatch> for DataBlock {
    type Error = ErrorCode;

    fn try_from(v: RecordBatch) -> Result<DataBlock> {
        let schema  = v.schema().clone();
        let series = v.columns().iter().map(|c| c.into_data_column()).collect();
        Ok(DataBlock::create_by_array(schema, series))
    }
}

impl fmt::Debug for DataBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted = pretty_format_blocks(&[self.clone()]).expect("Pretty format batches error");
        let lines: Vec<&str> = formatted.trim().lines().collect();
        write!(f, "\n{:#?}\n", lines)
    }
}
