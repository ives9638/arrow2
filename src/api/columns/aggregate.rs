use crate::api::prelude::{ArrowError, DataColumn, DataType, Result};
use crate::api::scalar::DataValue;
use crate::array::{
    BooleanArray, Float32Array, Float64Array, Int128Array, Int16Array, Int32Array, Int64Array,
    Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array, Utf8Array,
};
use crate::compute::aggregate;
use crate::datatypes::IntervalUnit;

impl DataColumn {
    #[inline]
    pub fn max(&self) -> Result<DataValue> {
        match self.data_type() {
            DataType::Boolean => Ok(aggregate::max_boolean(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap(),
            )
            .into()),
            DataType::Int8 => Ok(aggregate::max_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Int16 => Ok(aggregate::max_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Int32
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Interval(IntervalUnit::YearMonth) => Ok(aggregate::max_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Int64
            | DataType::Timestamp(_, None)
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Duration(_) => Ok(aggregate::max_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt8 => Ok(aggregate::max_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt16 => Ok(aggregate::max_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt32 => Ok(aggregate::max_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt64 => Ok(aggregate::max_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Float16 => unreachable!(),
            DataType::Float32 => Ok(aggregate::max_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Float64 => Ok(aggregate::max_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Interval(IntervalUnit::DayTime) => unreachable!(),
            DataType::Utf8 => Ok(aggregate::max_string(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap(),
            )
            .into()),
            DataType::LargeUtf8 => Ok(aggregate::max_string(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Utf8Array<i64>>()
                    .unwrap(),
            )
            .into()),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Comparison between {:?} is not supported",
                self.data_type()
            ))),
        }
    }
    #[inline]
    pub fn min(&self) -> Result<DataValue> {
        match self.data_type() {
            DataType::Boolean => Ok(aggregate::min_boolean(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap(),
            )
            .into()),
            DataType::Int8 => Ok(aggregate::min_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Int16 => Ok(aggregate::min_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Int32
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Interval(IntervalUnit::YearMonth) => Ok(aggregate::min_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Int64
            | DataType::Timestamp(_, None)
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Duration(_) => Ok(aggregate::min_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt8 => Ok(aggregate::min_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt16 => Ok(aggregate::min_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt32 => Ok(aggregate::min_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt64 => Ok(aggregate::min_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Float16 => unreachable!(),
            DataType::Float32 => Ok(aggregate::max_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Float64 => Ok(aggregate::min_primitive(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Interval(IntervalUnit::DayTime) => unreachable!(),
            DataType::Utf8 => Ok(aggregate::min_string(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap(),
            )
            .into()),
            DataType::LargeUtf8 => Ok(aggregate::min_string(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Utf8Array<i64>>()
                    .unwrap(),
            )
            .into()),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Comparison between {:?} is not supported",
                self.data_type()
            ))),
        }
    }
    #[inline]
    pub fn sum(&self) -> Result<DataValue> {
        match self.data_type() {
            DataType::Int8 => Ok(aggregate::sum(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Int16 => Ok(aggregate::sum(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Int32 => Ok(aggregate::sum(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Int64 => Ok(aggregate::sum(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt8 => Ok(aggregate::sum(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt16 => Ok(aggregate::sum(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt32 => Ok(aggregate::sum(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap(),
            )
            .into()),
            DataType::UInt64 => Ok(aggregate::sum(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Float16 => unreachable!(),
            DataType::Float32 => Ok(aggregate::sum(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap(),
            )
            .into()),
            DataType::Float64 => Ok(aggregate::sum(
                self.get_array_ref()?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap(),
            )
            .into()),

            _ => Err(ArrowError::NotYetImplemented(format!(
                "Comparison between {:?} is not supported",
                self.data_type()
            ))),
        }
    }

    #[inline]
    pub fn get_array_memory_size(&self) -> usize {
        aggregate::estimated_bytes_size(self.get_array_ref().unwrap().as_ref())
    }

}
