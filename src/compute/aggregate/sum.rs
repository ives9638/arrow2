use std::{iter::Sum, ops::AddAssign};

use multiversion::multiversion;

use crate::datatypes::{DataType, IntervalUnit};
use crate::error::{ArrowError, Result};
use crate::scalar::*;
use crate::types::{BitChunkIter, NativeType};
use crate::{
    array::{Array, PrimitiveArray},
    bitmap::Bitmap,
};

#[multiversion]
#[clone(target = "x86_64+avx")]
fn nonnull_sum<T: NativeType + AddAssign + Sum>(values: &[T]) -> T {
    let chunks = values.chunks_exact(T::LANES);
    let remainder = chunks.remainder();

    let sum = chunks.fold(T::new_simd(), |mut acc, chunk| {
        let chunk = T::from_slice(chunk);
        for i in 0..T::LANES {
            acc[i] += chunk[i];
        }
        acc
    });

    let mut reduced: T = remainder.iter().copied().sum();

    for i in 0..T::LANES {
        reduced += sum[i];
    }
    reduced
}

/// # Panics
/// iff `values.len() != bitmap.len()` or the operation overflows.
#[multiversion]
#[clone(target = "x86_64+avx")]
fn null_sum<T: NativeType + AddAssign + Sum>(values: &[T], bitmap: &Bitmap) -> T {
    let mut chunks = values.chunks_exact(T::LANES);

    let mut validity_masks = bitmap.chunks::<T::SimdMask>();

    let sum = chunks.by_ref().zip(validity_masks.by_ref()).fold(
        T::new_simd(),
        |mut acc, (chunk, validity_chunk)| {
            let chunk = T::from_slice(chunk);
            let iter = BitChunkIter::new(validity_chunk, T::LANES);
            for (i, b) in (0..T::LANES).zip(iter) {
                acc[i] += if b { chunk[i] } else { T::default() };
            }
            acc
        },
    );

    let mut reduced: T = chunks
        .remainder()
        .iter()
        .zip(BitChunkIter::new(validity_masks.remainder(), T::LANES))
        .map(|(x, is_valid)| if is_valid { *x } else { T::default() })
        .sum();

    for i in 0..T::LANES {
        reduced += sum[i];
    }
    reduced
}

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
pub fn sum_primitive<T>(array: &PrimitiveArray<T>) -> Option<T>
where
    T: NativeType + Sum + AddAssign,
{
    let null_count = array.null_count();

    if null_count == array.len() {
        return None;
    }

    match array.validity() {
        None => Some(nonnull_sum(array.values())),
        Some(bitmap) => Some(null_sum(array.values(), bitmap)),
    }
}

macro_rules! dyn_sum {
    ($ty:ty, $array:expr) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();
        Box::new(PrimitiveScalar::<$ty>::new(
            $array.data_type().clone(),
            sum_primitive::<$ty>(array),
        ))
    }};
}

pub fn sum(array: &dyn Array) -> Result<Box<dyn Scalar>> {
    Ok(match array.data_type() {
        DataType::Int8 => dyn_sum!(i8, array),
        DataType::Int16 => dyn_sum!(i16, array),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            dyn_sum!(i32, array)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => dyn_sum!(i64, array),
        DataType::UInt8 => dyn_sum!(u8, array),
        DataType::UInt16 => dyn_sum!(u16, array),
        DataType::UInt32 => dyn_sum!(u32, array),
        DataType::UInt64 => dyn_sum!(u64, array),
        DataType::Decimal(_, _) => dyn_sum!(i128, array),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => dyn_sum!(f32, array),
        DataType::Float64 => dyn_sum!(f64, array),
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "The `sum` operator does not support type `{}`",
                array.data_type(),
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::super::super::arithmetics;
    use super::*;
    use crate::array::*;
    use crate::datatypes::DataType;

    #[test]
    fn test_primitive_array_sum() {
        let a = Primitive::from_slice(&[1, 2, 3, 4, 5]).to(DataType::Int32);
        assert_eq!(15, sum_primitive(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_float_sum() {
        let a = Primitive::from_slice(&[1.1f64, 2.2, 3.3, 4.4, 5.5]).to(DataType::Float64);
        assert!((16.5 - sum_primitive(&a).unwrap()).abs() < f64::EPSILON);
    }

    #[test]
    fn test_primitive_array_sum_with_nulls() {
        let a = Primitive::from(vec![None, Some(2), Some(3), None, Some(5)]).to(DataType::Int32);
        assert_eq!(10, sum_primitive(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_sum_all_nulls() {
        let a = Primitive::<i32>::from(vec![None, None, None]).to(DataType::Int32);
        assert_eq!(None, sum_primitive(&a));
    }

    #[test]
    fn test_primitive_array_sum_large_64() {
        let a: Int64Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(i) } else { None })
            .collect::<Primitive<i64>>()
            .to(DataType::Int64);
        let b: Int64Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(0) } else { Some(i) })
            .collect::<Primitive<i64>>()
            .to(DataType::Int64);
        // create an array that actually has non-zero values at the invalid indices
        let c = arithmetics::basic::add::add(&a, &b).unwrap();
        assert_eq!(
            Some((1..=100).filter(|i| i % 3 == 0).sum()),
            sum_primitive(&c)
        );
    }
}
