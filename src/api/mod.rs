use crate::api::prelude::{DataType, Arc};
use crate::api::prelude::array::*;
use crate::datatypes::IntervalUnit;
use crate::types::days_ms;


mod types;
pub mod prelude;
mod compute;

pub fn  empty_array(data_type: DataType) -> ArrayRef {
    match data_type {
        DataType::Null => Arc::new(NullArray::new_empty()),
        DataType::Boolean => Arc::new(BooleanArray::new_empty()),
        DataType::Int8 => Arc::new(PrimitiveArray::<i8>::new_empty(data_type)),
        DataType::Int16 => Arc::new(PrimitiveArray::<i16>::new_empty(data_type)),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            Arc::new(PrimitiveArray::<i32>::new_empty(data_type))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Arc::new(PrimitiveArray::<days_ms>::new_empty(data_type))
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Arc::new(PrimitiveArray::<i64>::new_empty(data_type)),
        DataType::Decimal(_, _) => Arc::new(PrimitiveArray::<i128>::new_empty(data_type)),
        DataType::UInt8 => Arc::new(PrimitiveArray::<u8>::new_empty(data_type)),
        DataType::UInt16 => Arc::new(PrimitiveArray::<u16>::new_empty(data_type)),
        DataType::UInt32 => Arc::new(PrimitiveArray::<u32>::new_empty(data_type)),
        DataType::UInt64 => Arc::new(PrimitiveArray::<u64>::new_empty(data_type)),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => Arc::new(PrimitiveArray::<f32>::new_empty(data_type)),
        DataType::Float64 => Arc::new(PrimitiveArray::<f64>::new_empty(data_type)),
        DataType::Binary => Arc::new(BinaryArray::<i32>::new_empty()),
        DataType::LargeBinary => Arc::new(BinaryArray::<i64>::new_empty()),
        DataType::FixedSizeBinary(_) => Arc::new(FixedSizeBinaryArray::new_empty(data_type)),
        DataType::Utf8 => Arc::new(Utf8Array::<i32>::new_empty()),
        DataType::LargeUtf8 => Arc::new(Utf8Array::<i64>::new_empty()),
        DataType::List(_) => Arc::new(ListArray::<i32>::new_empty(data_type)),
        DataType::LargeList(_) => Arc::new(ListArray::<i64>::new_empty(data_type)),
        DataType::FixedSizeList(_, _) => Arc::new(FixedSizeListArray::new_empty(data_type)),
        DataType::Struct(fields) => Arc::new(StructArray::new_empty(&fields)),
        DataType::Union(_) => unimplemented!(),
        DataType::Dictionary(key_type, value_type) => match key_type.as_ref() {
            DataType::Int8 => Arc::new(DictionaryArray::<i8>::new_empty(*value_type)),
            DataType::Int16 => Arc::new(DictionaryArray::<i16>::new_empty(*value_type)),
            DataType::Int32 => Arc::new(DictionaryArray::<i32>::new_empty(*value_type)),
            DataType::Int64 => Arc::new(DictionaryArray::<i64>::new_empty(*value_type)),
            DataType::UInt8 => Arc::new(DictionaryArray::<u8>::new_empty(*value_type)),
            DataType::UInt16 => Arc::new(DictionaryArray::<u16>::new_empty(*value_type)),
            DataType::UInt32 => Arc::new(DictionaryArray::<u32>::new_empty(*value_type)),
            DataType::UInt64 => Arc::new(DictionaryArray::<u64>::new_empty(*value_type)),
            _ => unreachable!(),
        },
    }
}


