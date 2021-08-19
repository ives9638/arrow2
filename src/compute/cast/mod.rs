// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{
    array::*,
    buffer::Buffer,
    datatypes::*,
    error::{ArrowError, Result},
};

mod binary_to;
mod boolean_to;
mod dictionary_to;
mod primitive_to;
mod timestamps;
mod utf8_to;

pub use binary_to::*;
pub use boolean_to::*;
pub use dictionary_to::*;
pub use primitive_to::*;
pub use timestamps::*;
pub use utf8_to::*;

/// options defining how Cast kernels behave
#[derive(Clone, Copy, Debug)]
struct CastOptions {
    /// default to false
    /// whether an overflowing cast should be converted to `None` (default), or be wrapped (i.e. `256i16 as u8 = 0` vectorized).
    /// Settings this to `true` is 5-6x faster for numeric types.
    wrapped: bool,
}

impl Default for CastOptions {
    fn default() -> Self {
        Self { wrapped: false }
    }
}

impl CastOptions {
    fn with_wrapped(&self, v: bool) -> Self {
        let mut option = *self;
        option.wrapped = v;
        option
    }
}

/// Returns true if this type is numeric: (UInt*, Unit*, or Float*).
fn is_numeric(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float32 | Float64
    )
}

macro_rules! primitive_dyn {
    ($from:expr, $expr:tt) => {{
        let from = $from.as_any().downcast_ref().unwrap();
        Ok(Box::new($expr(from)))
    }};
    ($from:expr, $expr:tt, $to:expr) => {{
        let from = $from.as_any().downcast_ref().unwrap();
        Ok(Box::new($expr(from, $to)))
    }};
    ($from:expr, $expr:tt, $from_t:expr, $to:expr) => {{
        let from = $from.as_any().downcast_ref().unwrap();
        Ok(Box::new($expr(from, $from_t, $to)))
    }};
    ($from:expr, $expr:tt, $arg1:expr, $arg2:expr, $arg3:expr) => {{
        let from = $from.as_any().downcast_ref().unwrap();
        Ok(Box::new($expr(from, $arg1, $arg2, $arg3)))
    }};
}

/// Return true if a value of type `from_type` can be cast into a
/// value of `to_type`. Note that such as cast may be lossy.
///
/// If this function returns true to stay consistent with the `cast` kernel below.
pub fn can_cast_types(from_type: &DataType, to_type: &DataType) -> bool {
    use self::DataType::*;
    if from_type == to_type {
        return true;
    }

    match (from_type, to_type) {
        (Struct(_), _) => false,
        (_, Struct(_)) => false,
        (List(list_from), List(list_to)) => {
            can_cast_types(list_from.data_type(), list_to.data_type())
        }
        (LargeList(list_from), LargeList(list_to)) => {
            can_cast_types(list_from.data_type(), list_to.data_type())
        }
        (List(list_from), LargeList(list_to)) if list_from == list_to => true,
        (LargeList(list_from), List(list_to)) if list_from == list_to => true,
        (_, List(list_to)) => can_cast_types(from_type, list_to.data_type()),
        (Dictionary(_, from_value_type), Dictionary(_, to_value_type)) => {
            can_cast_types(from_value_type, to_value_type)
        }
        (Dictionary(_, value_type), _) => can_cast_types(value_type, to_type),
        (_, Dictionary(_, value_type)) => can_cast_types(from_type, value_type),

        (_, Boolean) => is_numeric(from_type),
        (Boolean, _) => is_numeric(to_type) || to_type == &Utf8 || to_type == &LargeUtf8,

        (Utf8, Date32) => true,
        (Utf8, Date64) => true,
        (Utf8, Timestamp(TimeUnit::Nanosecond, None)) => true,
        (Utf8, LargeUtf8) => true,
        (Utf8, _) => is_numeric(to_type),
        (LargeUtf8, Date32) => true,
        (LargeUtf8, Date64) => true,
        (LargeUtf8, Timestamp(TimeUnit::Nanosecond, None)) => true,
        (LargeUtf8, Utf8) => true,
        (LargeUtf8, _) => is_numeric(to_type),
        (_, Utf8) => is_numeric(from_type) || from_type == &Binary,
        (_, LargeUtf8) => is_numeric(from_type) || from_type == &Binary,
        (Binary, LargeBinary) => true,
        (LargeBinary, Binary) => true,

        // start numeric casts
        (UInt8, UInt16) => true,
        (UInt8, UInt32) => true,
        (UInt8, UInt64) => true,
        (UInt8, Int8) => true,
        (UInt8, Int16) => true,
        (UInt8, Int32) => true,
        (UInt8, Int64) => true,
        (UInt8, Float32) => true,
        (UInt8, Float64) => true,

        (UInt16, UInt8) => true,
        (UInt16, UInt32) => true,
        (UInt16, UInt64) => true,
        (UInt16, Int8) => true,
        (UInt16, Int16) => true,
        (UInt16, Int32) => true,
        (UInt16, Int64) => true,
        (UInt16, Float32) => true,
        (UInt16, Float64) => true,

        (UInt32, UInt8) => true,
        (UInt32, UInt16) => true,
        (UInt32, UInt64) => true,
        (UInt32, Int8) => true,
        (UInt32, Int16) => true,
        (UInt32, Int32) => true,
        (UInt32, Int64) => true,
        (UInt32, Float32) => true,
        (UInt32, Float64) => true,

        (UInt64, UInt8) => true,
        (UInt64, UInt16) => true,
        (UInt64, UInt32) => true,
        (UInt64, Int8) => true,
        (UInt64, Int16) => true,
        (UInt64, Int32) => true,
        (UInt64, Int64) => true,
        (UInt64, Float32) => true,
        (UInt64, Float64) => true,

        (Int8, UInt8) => true,
        (Int8, UInt16) => true,
        (Int8, UInt32) => true,
        (Int8, UInt64) => true,
        (Int8, Int16) => true,
        (Int8, Int32) => true,
        (Int8, Int64) => true,
        (Int8, Float32) => true,
        (Int8, Float64) => true,

        (Int16, UInt8) => true,
        (Int16, UInt16) => true,
        (Int16, UInt32) => true,
        (Int16, UInt64) => true,
        (Int16, Int8) => true,
        (Int16, Int32) => true,
        (Int16, Int64) => true,
        (Int16, Float32) => true,
        (Int16, Float64) => true,

        (Int32, UInt8) => true,
        (Int32, UInt16) => true,
        (Int32, UInt32) => true,
        (Int32, UInt64) => true,
        (Int32, Int8) => true,
        (Int32, Int16) => true,
        (Int32, Int64) => true,
        (Int32, Float32) => true,
        (Int32, Float64) => true,

        (Int64, UInt8) => true,
        (Int64, UInt16) => true,
        (Int64, UInt32) => true,
        (Int64, UInt64) => true,
        (Int64, Int8) => true,
        (Int64, Int16) => true,
        (Int64, Int32) => true,
        (Int64, Float32) => true,
        (Int64, Float64) => true,

        (Float32, UInt8) => true,
        (Float32, UInt16) => true,
        (Float32, UInt32) => true,
        (Float32, UInt64) => true,
        (Float32, Int8) => true,
        (Float32, Int16) => true,
        (Float32, Int32) => true,
        (Float32, Int64) => true,
        (Float32, Float64) => true,

        (Float64, UInt8) => true,
        (Float64, UInt16) => true,
        (Float64, UInt32) => true,
        (Float64, UInt64) => true,
        (Float64, Int8) => true,
        (Float64, Int16) => true,
        (Float64, Int32) => true,
        (Float64, Int64) => true,
        (Float64, Float32) => true,
        // end numeric casts

        // temporal casts
        (Int32, Date32) => true,
        (Int32, Time32(_)) => true,
        (Date32, Int32) => true,
        (Time32(_), Int32) => true,
        (Int64, Date64) => true,
        (Int64, Time64(_)) => true,
        (Date64, Int64) => true,
        (Time64(_), Int64) => true,
        (Date32, Date64) => true,
        (Date64, Date32) => true,
        (Time32(TimeUnit::Second), Time32(TimeUnit::Millisecond)) => true,
        (Time32(TimeUnit::Millisecond), Time32(TimeUnit::Second)) => true,
        (Time32(_), Time64(_)) => true,
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)) => true,
        (Time64(TimeUnit::Nanosecond), Time64(TimeUnit::Microsecond)) => true,
        (Time64(_), Time32(to_unit)) => {
            matches!(to_unit, TimeUnit::Second | TimeUnit::Millisecond)
        }
        (Timestamp(_, _), Int64) => true,
        (Int64, Timestamp(_, _)) => true,
        (Timestamp(_, _), Timestamp(_, _)) => true,
        (Timestamp(_, _), Date32) => true,
        (Timestamp(_, _), Date64) => true,
        (Int64, Duration(_)) => true,
        (Duration(_), Int64) => true,
        (Null, Int32) => true,
        (_, _) => false,
    }
}

fn cast_list<O: Offset>(
    array: &ListArray<O>,
    to_type: &DataType,
    options: CastOptions,
) -> Result<ListArray<O>> {
    let values = array.values();
    let new_values = cast_with_options(
        values.as_ref(),
        ListArray::<O>::get_child_type(to_type),
        options,
    )?
    .into();

    Ok(ListArray::<O>::from_data(
        to_type.clone(),
        array.offsets().clone(),
        new_values,
        array.validity().clone(),
    ))
}

fn cast_list_to_large_list(array: &ListArray<i32>, to_type: &DataType) -> ListArray<i64> {
    let offsets = array.offsets();
    let offsets = offsets.iter().map(|x| *x as i64);
    let offets = Buffer::from_trusted_len_iter(offsets);

    ListArray::<i64>::from_data(
        to_type.clone(),
        offets,
        array.values().clone(),
        array.validity().clone(),
    )
}

fn cast_large_to_list(array: &ListArray<i64>, to_type: &DataType) -> ListArray<i32> {
    let offsets = array.offsets();
    let offsets = offsets.iter().map(|x| *x as i32);
    let offets = Buffer::from_trusted_len_iter(offsets);

    ListArray::<i32>::from_data(
        to_type.clone(),
        offets,
        array.values().clone(),
        array.validity().clone(),
    )
}

/// Cast `array` to the provided data type and return a new [`Array`] with
/// type `to_type`, if possible.
///
/// Behavior:
/// * PrimitiveArray to PrimitiveArray: overflowing cast will be None
/// * Boolean to Utf8: `true` => '1', `false` => `0`
/// * Utf8 to numeric: strings that can't be parsed to numbers return null, float strings
///   in integer casts return null
/// * Numeric to boolean: 0 returns `false`, any other value returns `true`
/// * List to List: the underlying data type is cast
/// * PrimitiveArray to List: a list array with 1 value per slot is created
/// * Date32 and Date64: precision lost when going to higher interval
/// * Time32 and Time64: precision lost when going to higher interval
/// * Timestamp and Date{32|64}: precision lost when going to higher interval
/// * Temporal to/from backing primitive: zero-copy with data type change
/// Unsupported Casts
/// * To or from `StructArray`
/// * List to primitive
/// * Utf8 to boolean
/// * Interval and duration
pub fn cast(array: &dyn Array, to_type: &DataType) -> Result<Box<dyn Array>> {
    cast_with_options(array, to_type, CastOptions { wrapped: false })
}

/// Similar to [`cast`], but overflowing cast is wrapped
/// Behavior:
/// * PrimitiveArray to PrimitiveArray: overflowing cast will be wrapped (i.e. `256i16 as u8 = 0` vectorized).
pub fn wrapping_cast(array: &dyn Array, to_type: &DataType) -> Result<Box<dyn Array>> {
    cast_with_options(array, to_type, CastOptions { wrapped: true })
}

#[inline]
fn cast_with_options(
    array: &dyn Array,
    to_type: &DataType,
    options: CastOptions,
) -> Result<Box<dyn Array>> {
    use DataType::*;
    let from_type = array.data_type();

    // clone array if types are the same
    if from_type == to_type {
        return Ok(clone(array));
    }

    let as_options = options.with_wrapped(true);
    match (from_type, to_type) {
        (Null, Int32) => Ok(new_null_array(to_type.clone(), array.len())),
        (Struct(_), _) => Err(ArrowError::NotYetImplemented(
            "Cannot cast from struct to other types".to_string(),
        )),
        (_, Struct(_)) => Err(ArrowError::NotYetImplemented(
            "Cannot cast to struct from other types".to_string(),
        )),
        (List(_), List(_)) => {
            cast_list::<i32>(array.as_any().downcast_ref().unwrap(), to_type, options)
                .map(|x| Box::new(x) as Box<dyn Array>)
        }
        (LargeList(_), LargeList(_)) => {
            cast_list::<i64>(array.as_any().downcast_ref().unwrap(), to_type, options)
                .map(|x| Box::new(x) as Box<dyn Array>)
        }
        (List(lhs), LargeList(rhs)) if lhs == rhs => Ok(cast_list_to_large_list(
            array.as_any().downcast_ref().unwrap(),
            to_type,
        ))
        .map(|x| Box::new(x) as Box<dyn Array>),
        (LargeList(lhs), List(rhs)) if lhs == rhs => Ok(cast_large_to_list(
            array.as_any().downcast_ref().unwrap(),
            to_type,
        ))
        .map(|x| Box::new(x) as Box<dyn Array>),

        (_, List(to)) => {
            // cast primitive to list's primitive
            let values = cast_with_options(array, to.data_type(), options)?.into();
            // create offsets, where if array.len() = 2, we have [0,1,2]
            let offsets =
                unsafe { Buffer::from_trusted_len_iter_unchecked(0..=array.len() as i32) };

            let list_array = ListArray::<i32>::from_data(to_type.clone(), offsets, values, None);

            Ok(Box::new(list_array))
        }

        (Dictionary(index_type, _), _) => match **index_type {
            DataType::Int8 => dictionary_cast_dyn::<i8>(array, to_type, options),
            DataType::Int16 => dictionary_cast_dyn::<i16>(array, to_type, options),
            DataType::Int32 => dictionary_cast_dyn::<i32>(array, to_type, options),
            DataType::Int64 => dictionary_cast_dyn::<i64>(array, to_type, options),
            DataType::UInt8 => dictionary_cast_dyn::<u8>(array, to_type, options),
            DataType::UInt16 => dictionary_cast_dyn::<u16>(array, to_type, options),
            DataType::UInt32 => dictionary_cast_dyn::<u32>(array, to_type, options),
            DataType::UInt64 => dictionary_cast_dyn::<u64>(array, to_type, options),
            _ => unreachable!(),
        },
        (_, Dictionary(index_type, value_type)) => match **index_type {
            DataType::Int8 => cast_to_dictionary::<i8>(array, value_type, options),
            DataType::Int16 => cast_to_dictionary::<i16>(array, value_type, options),
            DataType::Int32 => cast_to_dictionary::<i32>(array, value_type, options),
            DataType::Int64 => cast_to_dictionary::<i64>(array, value_type, options),
            DataType::UInt8 => cast_to_dictionary::<u8>(array, value_type, options),
            DataType::UInt16 => cast_to_dictionary::<u16>(array, value_type, options),
            DataType::UInt32 => cast_to_dictionary::<u32>(array, value_type, options),
            DataType::UInt64 => cast_to_dictionary::<u64>(array, value_type, options),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from type {:?} to dictionary type {:?} not supported",
                from_type, to_type,
            ))),
        },
        (_, Boolean) => match from_type {
            UInt8 => primitive_to_boolean_dyn::<u8>(array),
            UInt16 => primitive_to_boolean_dyn::<u16>(array),
            UInt32 => primitive_to_boolean_dyn::<u32>(array),
            UInt64 => primitive_to_boolean_dyn::<u64>(array),
            Int8 => primitive_to_boolean_dyn::<i8>(array),
            Int16 => primitive_to_boolean_dyn::<i16>(array),
            Int32 => primitive_to_boolean_dyn::<i32>(array),
            Int64 => primitive_to_boolean_dyn::<i64>(array),
            Float32 => primitive_to_boolean_dyn::<f32>(array),
            Float64 => primitive_to_boolean_dyn::<f64>(array),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },
        (Boolean, _) => match to_type {
            UInt8 => boolean_to_primitive_dyn::<u8>(array),
            UInt16 => boolean_to_primitive_dyn::<u16>(array),
            UInt32 => boolean_to_primitive_dyn::<u32>(array),
            UInt64 => boolean_to_primitive_dyn::<u64>(array),
            Int8 => boolean_to_primitive_dyn::<i8>(array),
            Int16 => boolean_to_primitive_dyn::<i16>(array),
            Int32 => boolean_to_primitive_dyn::<i32>(array),
            Int64 => boolean_to_primitive_dyn::<i64>(array),
            Float32 => boolean_to_primitive_dyn::<f32>(array),
            Float64 => boolean_to_primitive_dyn::<f64>(array),
            Utf8 => boolean_to_utf8_dyn::<i32>(array),
            LargeUtf8 => boolean_to_utf8_dyn::<i64>(array),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        (Utf8, _) => match to_type {
            UInt8 => utf8_to_primitive_dyn::<i32, u8>(array, to_type),
            UInt16 => utf8_to_primitive_dyn::<i32, u16>(array, to_type),
            UInt32 => utf8_to_primitive_dyn::<i32, u32>(array, to_type),
            UInt64 => utf8_to_primitive_dyn::<i32, u64>(array, to_type),
            Int8 => utf8_to_primitive_dyn::<i32, i8>(array, to_type),
            Int16 => utf8_to_primitive_dyn::<i32, i16>(array, to_type),
            Int32 => utf8_to_primitive_dyn::<i32, i32>(array, to_type),
            Int64 => utf8_to_primitive_dyn::<i32, i64>(array, to_type),
            Float32 => utf8_to_primitive_dyn::<i32, f32>(array, to_type),
            Float64 => utf8_to_primitive_dyn::<i32, f64>(array, to_type),
            Date32 => utf8_to_date32_dyn::<i32>(array),
            Date64 => utf8_to_date64_dyn::<i32>(array),
            LargeUtf8 => Ok(Box::new(utf8_to_large_utf8(
                array.as_any().downcast_ref().unwrap(),
            ))),
            Timestamp(TimeUnit::Nanosecond, None) => utf8_to_timestamp_ns_dyn::<i32>(array),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },
        (LargeUtf8, _) => match to_type {
            UInt8 => utf8_to_primitive_dyn::<i64, u8>(array, to_type),
            UInt16 => utf8_to_primitive_dyn::<i64, u16>(array, to_type),
            UInt32 => utf8_to_primitive_dyn::<i64, u32>(array, to_type),
            UInt64 => utf8_to_primitive_dyn::<i64, u64>(array, to_type),
            Int8 => utf8_to_primitive_dyn::<i64, i8>(array, to_type),
            Int16 => utf8_to_primitive_dyn::<i64, i16>(array, to_type),
            Int32 => utf8_to_primitive_dyn::<i64, i32>(array, to_type),
            Int64 => utf8_to_primitive_dyn::<i64, i64>(array, to_type),
            Float32 => utf8_to_primitive_dyn::<i64, f32>(array, to_type),
            Float64 => utf8_to_primitive_dyn::<i64, f64>(array, to_type),
            Date32 => utf8_to_date32_dyn::<i64>(array),
            Date64 => utf8_to_date64_dyn::<i64>(array),
            Utf8 => utf8_large_to_utf8(array.as_any().downcast_ref().unwrap())
                .map(|x| Box::new(x) as Box<dyn Array>),
            Timestamp(TimeUnit::Nanosecond, None) => utf8_to_timestamp_ns_dyn::<i64>(array),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        (_, Utf8) => match from_type {
            UInt8 => primitive_to_utf8_dyn::<u8, i32>(array),
            UInt16 => primitive_to_utf8_dyn::<u16, i32>(array),
            UInt32 => primitive_to_utf8_dyn::<u32, i32>(array),
            UInt64 => primitive_to_utf8_dyn::<u64, i32>(array),
            Int8 => primitive_to_utf8_dyn::<i8, i32>(array),
            Int16 => primitive_to_utf8_dyn::<i16, i32>(array),
            Int32 => primitive_to_utf8_dyn::<i32, i32>(array),
            Int64 => primitive_to_utf8_dyn::<i64, i32>(array),
            Float32 => primitive_to_utf8_dyn::<f32, i32>(array),
            Float64 => primitive_to_utf8_dyn::<f64, i32>(array),
            Binary => {
                let array = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();

                // perf todo: the offsets are equal; we can speed-up this
                let iter = array
                    .iter()
                    .map(|x| x.and_then(|x| std::str::from_utf8(x).ok()));

                let array = Utf8Array::<i32>::from_trusted_len_iter(iter);
                Ok(Box::new(array))
            }
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        (_, LargeUtf8) => match from_type {
            UInt8 => primitive_to_utf8_dyn::<u8, i64>(array),
            UInt16 => primitive_to_utf8_dyn::<u16, i64>(array),
            UInt32 => primitive_to_utf8_dyn::<u32, i64>(array),
            UInt64 => primitive_to_utf8_dyn::<u64, i64>(array),
            Int8 => primitive_to_utf8_dyn::<i8, i64>(array),
            Int16 => primitive_to_utf8_dyn::<i16, i64>(array),
            Int32 => primitive_to_utf8_dyn::<i32, i64>(array),
            Int64 => primitive_to_utf8_dyn::<i64, i64>(array),
            Float32 => primitive_to_utf8_dyn::<f32, i64>(array),
            Float64 => primitive_to_utf8_dyn::<f64, i64>(array),
            Binary => {
                let array = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();

                // perf todo: the offsets are equal; we can speed-up this
                let iter = array
                    .iter()
                    .map(|x| x.and_then(|x| std::str::from_utf8(x).ok()));

                let array = Utf8Array::<i64>::from_trusted_len_iter(iter);
                Ok(Box::new(array))
            }
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        (Binary, LargeBinary) => Ok(Box::new(binary_to_large_binary(
            array.as_any().downcast_ref().unwrap(),
        ))),
        (LargeBinary, Binary) => binary_large_to_binary(array.as_any().downcast_ref().unwrap())
            .map(|x| Box::new(x) as Box<dyn Array>),

        // start numeric casts
        (UInt8, UInt16) => primitive_to_primitive_dyn::<u8, u16>(array, to_type, as_options),
        (UInt8, UInt32) => primitive_to_primitive_dyn::<u8, u32>(array, to_type, as_options),
        (UInt8, UInt64) => primitive_to_primitive_dyn::<u8, u64>(array, to_type, as_options),
        (UInt8, Int8) => primitive_to_primitive_dyn::<u8, i8>(array, to_type, options),
        (UInt8, Int16) => primitive_to_primitive_dyn::<u8, i16>(array, to_type, options),
        (UInt8, Int32) => primitive_to_primitive_dyn::<u8, i32>(array, to_type, options),
        (UInt8, Int64) => primitive_to_primitive_dyn::<u8, i64>(array, to_type, options),
        (UInt8, Float32) => primitive_to_primitive_dyn::<u8, f32>(array, to_type, as_options),
        (UInt8, Float64) => primitive_to_primitive_dyn::<u8, f64>(array, to_type, as_options),

        (UInt16, UInt8) => primitive_to_primitive_dyn::<u16, u8>(array, to_type, options),
        (UInt16, UInt32) => primitive_to_primitive_dyn::<u16, u32>(array, to_type, as_options),
        (UInt16, UInt64) => primitive_to_primitive_dyn::<u16, u64>(array, to_type, as_options),
        (UInt16, Int8) => primitive_to_primitive_dyn::<u16, i8>(array, to_type, options),
        (UInt16, Int16) => primitive_to_primitive_dyn::<u16, i16>(array, to_type, options),
        (UInt16, Int32) => primitive_to_primitive_dyn::<u16, i32>(array, to_type, options),
        (UInt16, Int64) => primitive_to_primitive_dyn::<u16, i64>(array, to_type, options),
        (UInt16, Float32) => primitive_to_primitive_dyn::<u16, f32>(array, to_type, as_options),
        (UInt16, Float64) => primitive_to_primitive_dyn::<u16, f64>(array, to_type, as_options),

        (UInt32, UInt8) => primitive_to_primitive_dyn::<u32, u8>(array, to_type, options),
        (UInt32, UInt16) => primitive_to_primitive_dyn::<u32, u16>(array, to_type, options),
        (UInt32, UInt64) => primitive_to_primitive_dyn::<u32, u64>(array, to_type, as_options),
        (UInt32, Int8) => primitive_to_primitive_dyn::<u32, i8>(array, to_type, options),
        (UInt32, Int16) => primitive_to_primitive_dyn::<u32, i16>(array, to_type, options),
        (UInt32, Int32) => primitive_to_primitive_dyn::<u32, i32>(array, to_type, options),
        (UInt32, Int64) => primitive_to_primitive_dyn::<u32, i64>(array, to_type, options),
        (UInt32, Float32) => primitive_to_primitive_dyn::<u32, f32>(array, to_type, as_options),
        (UInt32, Float64) => primitive_to_primitive_dyn::<u32, f64>(array, to_type, as_options),

        (UInt64, UInt8) => primitive_to_primitive_dyn::<u64, u8>(array, to_type, options),
        (UInt64, UInt16) => primitive_to_primitive_dyn::<u64, u16>(array, to_type, options),
        (UInt64, UInt32) => primitive_to_primitive_dyn::<u64, u32>(array, to_type, options),
        (UInt64, Int8) => primitive_to_primitive_dyn::<u64, i8>(array, to_type, options),
        (UInt64, Int16) => primitive_to_primitive_dyn::<u64, i16>(array, to_type, options),
        (UInt64, Int32) => primitive_to_primitive_dyn::<u64, i32>(array, to_type, options),
        (UInt64, Int64) => primitive_to_primitive_dyn::<u64, i64>(array, to_type, options),
        (UInt64, Float32) => primitive_to_primitive_dyn::<u64, f32>(array, to_type, as_options),
        (UInt64, Float64) => primitive_to_primitive_dyn::<u64, f64>(array, to_type, as_options),

        (Int8, UInt8) => primitive_to_primitive_dyn::<i8, u8>(array, to_type, options),
        (Int8, UInt16) => primitive_to_primitive_dyn::<i8, u16>(array, to_type, options),
        (Int8, UInt32) => primitive_to_primitive_dyn::<i8, u32>(array, to_type, options),
        (Int8, UInt64) => primitive_to_primitive_dyn::<i8, u64>(array, to_type, options),
        (Int8, Int16) => primitive_to_primitive_dyn::<i8, i16>(array, to_type, as_options),
        (Int8, Int32) => primitive_to_primitive_dyn::<i8, i32>(array, to_type, as_options),
        (Int8, Int64) => primitive_to_primitive_dyn::<i8, i64>(array, to_type, as_options),
        (Int8, Float32) => primitive_to_primitive_dyn::<i8, f32>(array, to_type, as_options),
        (Int8, Float64) => primitive_to_primitive_dyn::<i8, f64>(array, to_type, as_options),

        (Int16, UInt8) => primitive_to_primitive_dyn::<i16, u8>(array, to_type, options),
        (Int16, UInt16) => primitive_to_primitive_dyn::<i16, u16>(array, to_type, options),
        (Int16, UInt32) => primitive_to_primitive_dyn::<i16, u32>(array, to_type, options),
        (Int16, UInt64) => primitive_to_primitive_dyn::<i16, u64>(array, to_type, options),
        (Int16, Int8) => primitive_to_primitive_dyn::<i16, i8>(array, to_type, options),
        (Int16, Int32) => primitive_to_primitive_dyn::<i16, i32>(array, to_type, as_options),
        (Int16, Int64) => primitive_to_primitive_dyn::<i16, i64>(array, to_type, as_options),
        (Int16, Float32) => primitive_to_primitive_dyn::<i16, f32>(array, to_type, as_options),
        (Int16, Float64) => primitive_to_primitive_dyn::<i16, f64>(array, to_type, as_options),

        (Int32, UInt8) => primitive_to_primitive_dyn::<i32, u8>(array, to_type, options),
        (Int32, UInt16) => primitive_to_primitive_dyn::<i32, u16>(array, to_type, options),
        (Int32, UInt32) => primitive_to_primitive_dyn::<i32, u32>(array, to_type, options),
        (Int32, UInt64) => primitive_to_primitive_dyn::<i32, u64>(array, to_type, options),
        (Int32, Int8) => primitive_to_primitive_dyn::<i32, i8>(array, to_type, options),
        (Int32, Int16) => primitive_to_primitive_dyn::<i32, i16>(array, to_type, options),
        (Int32, Int64) => primitive_to_primitive_dyn::<i32, i64>(array, to_type, as_options),
        (Int32, Float32) => primitive_to_primitive_dyn::<i32, f32>(array, to_type, as_options),
        (Int32, Float64) => primitive_to_primitive_dyn::<i32, f64>(array, to_type, as_options),

        (Int64, UInt8) => primitive_to_primitive_dyn::<i64, u8>(array, to_type, options),
        (Int64, UInt16) => primitive_to_primitive_dyn::<i64, u16>(array, to_type, options),
        (Int64, UInt32) => primitive_to_primitive_dyn::<i64, u32>(array, to_type, options),
        (Int64, UInt64) => primitive_to_primitive_dyn::<i64, u64>(array, to_type, options),
        (Int64, Int8) => primitive_to_primitive_dyn::<i64, i8>(array, to_type, options),
        (Int64, Int16) => primitive_to_primitive_dyn::<i64, i16>(array, to_type, options),
        (Int64, Int32) => primitive_to_primitive_dyn::<i64, i32>(array, to_type, options),
        (Int64, Float32) => primitive_to_primitive_dyn::<i64, f32>(array, to_type, options),
        (Int64, Float64) => primitive_to_primitive_dyn::<i64, f64>(array, to_type, as_options),

        (Float32, UInt8) => primitive_to_primitive_dyn::<f32, u8>(array, to_type, options),
        (Float32, UInt16) => primitive_to_primitive_dyn::<f32, u16>(array, to_type, options),
        (Float32, UInt32) => primitive_to_primitive_dyn::<f32, u32>(array, to_type, options),
        (Float32, UInt64) => primitive_to_primitive_dyn::<f32, u64>(array, to_type, options),
        (Float32, Int8) => primitive_to_primitive_dyn::<f32, i8>(array, to_type, options),
        (Float32, Int16) => primitive_to_primitive_dyn::<f32, i16>(array, to_type, options),
        (Float32, Int32) => primitive_to_primitive_dyn::<f32, i32>(array, to_type, options),
        (Float32, Int64) => primitive_to_primitive_dyn::<f32, i64>(array, to_type, options),
        (Float32, Float64) => primitive_to_primitive_dyn::<f32, f64>(array, to_type, as_options),

        (Float64, UInt8) => primitive_to_primitive_dyn::<f64, u8>(array, to_type, options),
        (Float64, UInt16) => primitive_to_primitive_dyn::<f64, u16>(array, to_type, options),
        (Float64, UInt32) => primitive_to_primitive_dyn::<f64, u32>(array, to_type, options),
        (Float64, UInt64) => primitive_to_primitive_dyn::<f64, u64>(array, to_type, options),
        (Float64, Int8) => primitive_to_primitive_dyn::<f64, i8>(array, to_type, options),
        (Float64, Int16) => primitive_to_primitive_dyn::<f64, i16>(array, to_type, options),
        (Float64, Int32) => primitive_to_primitive_dyn::<f64, i32>(array, to_type, options),
        (Float64, Int64) => primitive_to_primitive_dyn::<f64, i64>(array, to_type, options),
        (Float64, Float32) => primitive_to_primitive_dyn::<f64, f32>(array, to_type, options),
        // end numeric casts

        // temporal casts
        (Int32, Date32) => primitive_to_same_primitive_dyn::<i32>(array, to_type),
        (Int32, Time32(TimeUnit::Second)) => primitive_to_same_primitive_dyn::<i32>(array, to_type),
        (Int32, Time32(TimeUnit::Millisecond)) => {
            primitive_to_same_primitive_dyn::<i32>(array, to_type)
        }
        // No support for microsecond/nanosecond with i32
        (Date32, Int32) => primitive_to_same_primitive_dyn::<i32>(array, to_type),
        (Time32(_), Int32) => primitive_to_same_primitive_dyn::<i32>(array, to_type),
        (Int64, Date64) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        // No support for second/milliseconds with i64
        (Int64, Time64(TimeUnit::Microsecond)) => {
            primitive_to_same_primitive_dyn::<i64>(array, to_type)
        }
        (Int64, Time64(TimeUnit::Nanosecond)) => {
            primitive_to_same_primitive_dyn::<i64>(array, to_type)
        }

        (Date64, Int64) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        (Time64(_), Int64) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        (Date32, Date64) => primitive_dyn!(array, date32_to_date64),
        (Date64, Date32) => primitive_dyn!(array, date64_to_date32),
        (Time32(TimeUnit::Second), Time32(TimeUnit::Millisecond)) => {
            primitive_dyn!(array, time32s_to_time32ms)
        }
        (Time32(TimeUnit::Millisecond), Time32(TimeUnit::Second)) => {
            primitive_dyn!(array, time32ms_to_time32s)
        }
        (Time32(from_unit), Time64(to_unit)) => {
            primitive_dyn!(array, time32_to_time64, from_unit, to_unit)
        }
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)) => {
            primitive_dyn!(array, time64us_to_time64ns)
        }
        (Time64(TimeUnit::Nanosecond), Time64(TimeUnit::Microsecond)) => {
            primitive_dyn!(array, time64ns_to_time64us)
        }
        (Time64(from_unit), Time32(to_unit)) => {
            primitive_dyn!(array, time64_to_time32, from_unit, to_unit)
        }
        (Timestamp(_, _), Int64) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        (Int64, Timestamp(_, _)) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        (Timestamp(from_unit, tz1), Timestamp(to_unit, tz2)) if tz1 == tz2 => {
            primitive_dyn!(array, timestamp_to_timestamp, from_unit, to_unit, tz2)
        }
        (Timestamp(from_unit, _), Date32) => primitive_dyn!(array, timestamp_to_date32, from_unit),
        (Timestamp(from_unit, _), Date64) => primitive_dyn!(array, timestamp_to_date64, from_unit),

        (Int64, Duration(_)) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        (Duration(_), Int64) => primitive_to_same_primitive_dyn::<i64>(array, to_type),

        // null to primitive/flat types
        //(Null, Int32) => Ok(Box::new(Int32Array::from(vec![None; array.len()]))),
        (_, _) => Err(ArrowError::NotYetImplemented(format!(
            "Casting from {:?} to {:?} not supported",
            from_type, to_type,
        ))),
    }
}

/// Attempts to encode an array into an `ArrayDictionary` with index
/// type K and value (dictionary) type value_type
///
/// K is the key type
fn cast_to_dictionary<K: DictionaryKey>(
    array: &dyn Array,
    dict_value_type: &DataType,
    options: CastOptions,
) -> Result<Box<dyn Array>> {
    let array = cast_with_options(array, dict_value_type, options)?;
    let array = array.as_ref();
    match *dict_value_type {
        DataType::Int8 => primitive_to_dictionary_dyn::<i8, K>(array),
        DataType::Int16 => primitive_to_dictionary_dyn::<i16, K>(array),
        DataType::Int32 => primitive_to_dictionary_dyn::<i32, K>(array),
        DataType::Int64 => primitive_to_dictionary_dyn::<i64, K>(array),
        DataType::UInt8 => primitive_to_dictionary_dyn::<u8, K>(array),
        DataType::UInt16 => primitive_to_dictionary_dyn::<u16, K>(array),
        DataType::UInt32 => primitive_to_dictionary_dyn::<u32, K>(array),
        DataType::UInt64 => primitive_to_dictionary_dyn::<u64, K>(array),
        DataType::Utf8 => utf8_to_dictionary_dyn::<i32, K>(array),
        DataType::LargeUtf8 => utf8_to_dictionary_dyn::<i64, K>(array),
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Unsupported output type for dictionary packing: {:?}",
            dict_value_type
        ))),
    }
}
