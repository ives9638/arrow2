use arrow2::{
    array::*,
    bitmap::{Bitmap, MutableBitmap},
    buffer::MutableBuffer,
    datatypes::DataType,
    error::Result,
};
use std::iter::FromIterator;

#[test]
fn from_and_into_data() {
    let a = MutablePrimitiveArray::from_data(
        DataType::Int32,
        MutableBuffer::from([1i32, 0]),
        Some(MutableBitmap::from([true, false])),
    );
    assert_eq!(a.len(), 2);
    let (a, b, c) = a.into_data();
    assert_eq!(a, DataType::Int32);
    assert_eq!(b, MutableBuffer::from([1i32, 0]));
    assert_eq!(c, Some(MutableBitmap::from([true, false])));
}

#[test]
fn to() {
    let a = MutablePrimitiveArray::from_data(
        DataType::Int32,
        MutableBuffer::from([1i32, 0]),
        Some(MutableBitmap::from([true, false])),
    );
    let a = a.to(DataType::Date32);
    assert_eq!(a.data_type(), &DataType::Date32);
}

#[test]
fn values_mut_slice() {
    let mut a = MutablePrimitiveArray::from_data(
        DataType::Int32,
        MutableBuffer::from([1i32, 0]),
        Some(MutableBitmap::from([true, false])),
    );
    let values = a.values_mut_slice();

    values[0] = 10;
    assert_eq!(a.values()[0], 10);
}

#[test]
fn push() {
    let mut a = MutablePrimitiveArray::<i32>::new();
    a.push(Some(1));
    a.push(None);
    a.push_null();
    assert_eq!(a.len(), 3);
    assert!(a.is_valid(0));
    assert!(!a.is_valid(1));
    assert!(!a.is_valid(2));

    assert_eq!(a.values(), &MutableBuffer::from([1, 0, 0]));
}

#[test]
fn set() {
    let mut a = MutablePrimitiveArray::<i32>::from([Some(1), None]);

    a.set(0, Some(2));
    a.set(1, Some(1));

    assert_eq!(a.len(), 2);
    assert!(a.is_valid(0));
    assert!(a.is_valid(1));

    assert_eq!(a.values(), &MutableBuffer::from([2, 1]));

    let mut a = MutablePrimitiveArray::<i32>::from_slice([1, 2]);

    a.set(0, Some(2));
    a.set(1, None);

    assert_eq!(a.len(), 2);
    assert!(a.is_valid(0));
    assert!(!a.is_valid(1));

    assert_eq!(a.values(), &MutableBuffer::from([2, 0]));
}

#[test]
fn from_iter() {
    let a = MutablePrimitiveArray::<i32>::from_iter((0..2).map(Some));
    assert_eq!(a.len(), 2);
    assert_eq!(a.validity(), &None);
}

#[test]
fn natural_arc() {
    let a = MutablePrimitiveArray::<i32>::from_slice(&[0, 1]).into_arc();
    assert_eq!(a.len(), 2);
}

#[test]
fn only_nulls() {
    let mut a = MutablePrimitiveArray::<i32>::new();
    a.push(None);
    a.push(None);
    let a: PrimitiveArray<i32> = a.into();
    assert_eq!(a.validity(), &Some(Bitmap::from([false, false])));
}

#[test]
fn from_trusted_len() {
    let a = MutablePrimitiveArray::<i32>::from_trusted_len_iter(vec![Some(1), None].into_iter());
    let a: PrimitiveArray<i32> = a.into();
    assert_eq!(a.validity(), &Some(Bitmap::from([true, false])));
}

#[test]
fn extend_trusted_len() {
    let mut a = MutablePrimitiveArray::<i32>::new();
    a.extend_trusted_len(vec![Some(1), Some(2)].into_iter());
    assert_eq!(a.validity(), &None);
    a.extend_trusted_len(vec![None, Some(4)].into_iter());
    assert_eq!(
        a.validity(),
        &Some(MutableBitmap::from([true, true, false, true]))
    );
    assert_eq!(a.values(), &MutableBuffer::<i32>::from([1, 2, 0, 4]));
}

#[test]
fn extend_trusted_len_values() {
    let mut a = MutablePrimitiveArray::<i32>::new();
    a.extend_trusted_len_values(vec![1, 2, 3].into_iter());
    assert_eq!(a.validity(), &None);
    assert_eq!(a.values(), &MutableBuffer::<i32>::from([1, 2, 3]));

    let mut a = MutablePrimitiveArray::<i32>::new();
    a.push(None);
    a.extend_trusted_len_values(vec![1, 2].into_iter());
    assert_eq!(
        a.validity(),
        &Some(MutableBitmap::from([false, true, true]))
    );
}

#[test]
fn extend_from_slice() {
    let mut a = MutablePrimitiveArray::<i32>::new();
    a.extend_from_slice(&[1, 2, 3]);
    assert_eq!(a.validity(), &None);
    assert_eq!(a.values(), &MutableBuffer::<i32>::from([1, 2, 3]));

    let mut a = MutablePrimitiveArray::<i32>::new();
    a.push(None);
    a.extend_from_slice(&[1, 2]);
    assert_eq!(
        a.validity(),
        &Some(MutableBitmap::from([false, true, true]))
    );
}

#[test]
fn set_validity() {
    let mut a = MutablePrimitiveArray::<i32>::new();
    a.extend_trusted_len(vec![Some(1), Some(2)].into_iter());
    assert_eq!(a.validity(), &None);
    a.set_validity(Some(MutableBitmap::from([false, true])));
    assert_eq!(a.validity(), &Some(MutableBitmap::from([false, true])));
}

#[test]
fn set_values() {
    let mut a = MutablePrimitiveArray::<i32>::from_slice([1, 2]);
    a.set_values(MutableBuffer::from([1, 3]));
    assert_eq!(a.values().as_slice(), [1, 3]);
}

#[test]
fn try_from_trusted_len_iter() {
    let iter = std::iter::repeat(Some(1)).take(2).map(Result::Ok);
    let a = MutablePrimitiveArray::try_from_trusted_len_iter(iter).unwrap();
    assert_eq!(a, MutablePrimitiveArray::from([Some(1), Some(1)]));
}

#[test]
#[should_panic]
fn wrong_data_type() {
    let values = MutableBuffer::from(b"abbb");
    MutablePrimitiveArray::from_data(DataType::Utf8, values, None);
}
