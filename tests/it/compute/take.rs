use std::sync::Arc;

use arrow2::compute::take::{can_take, take};
use arrow2::datatypes::{DataType, Field, IntervalUnit};
use arrow2::error::Result;
use arrow2::{array::*, bitmap::MutableBitmap, types::NativeType};

fn test_take_primitive<T>(
    data: &[Option<T>],
    indices: &Int32Array,
    expected_data: &[Option<T>],
    data_type: DataType,
) -> Result<()>
where
    T: NativeType,
{
    let output = PrimitiveArray::<T>::from(data).to(data_type.clone());
    let expected = PrimitiveArray::<T>::from(expected_data).to(data_type);
    let output = take(&output, indices)?;
    assert_eq!(expected, output.as_ref());
    Ok(())
}

#[test]
fn test_take_primitive_non_null_indices() {
    let indices = Int32Array::from_slice(&[0, 5, 3, 1, 4, 2]);
    test_take_primitive::<i8>(
        &[None, Some(2), Some(4), Some(6), Some(8), None],
        &indices,
        &[None, None, Some(6), Some(2), Some(8), Some(4)],
        DataType::Int8,
    )
    .unwrap();

    test_take_primitive::<i8>(
        &[Some(0), Some(2), Some(4), Some(6), Some(8), Some(10)],
        &indices,
        &[Some(0), Some(10), Some(6), Some(2), Some(8), Some(4)],
        DataType::Int8,
    )
    .unwrap();
}

#[test]
fn test_take_primitive_null_values() {
    let indices = Int32Array::from(&[Some(0), None, Some(3), Some(1), Some(4), Some(2)]);
    test_take_primitive::<i8>(
        &[Some(0), Some(2), Some(4), Some(6), Some(8), Some(10)],
        &indices,
        &[Some(0), None, Some(6), Some(2), Some(8), Some(4)],
        DataType::Int8,
    )
    .unwrap();

    test_take_primitive::<i8>(
        &[None, Some(2), Some(4), Some(6), Some(8), Some(10)],
        &indices,
        &[None, None, Some(6), Some(2), Some(8), Some(4)],
        DataType::Int8,
    )
    .unwrap();
}

fn create_test_struct() -> StructArray {
    let boolean = BooleanArray::from_slice(&[true, false, false, true]);
    let int = Int32Array::from_slice(&[42, 28, 19, 31]);
    let validity = vec![true, true, false, true]
        .into_iter()
        .collect::<MutableBitmap>()
        .into();
    let fields = vec![
        Field::new("a", DataType::Boolean, true),
        Field::new("b", DataType::Int32, true),
    ];
    StructArray::from_data(
        DataType::Struct(fields),
        vec![
            Arc::new(boolean) as Arc<dyn Array>,
            Arc::new(int) as Arc<dyn Array>,
        ],
        validity,
    )
}

#[test]
fn test_struct_with_nulls() {
    let array = create_test_struct();

    let indices = Int32Array::from(&[None, Some(3), Some(1), None, Some(0)]);

    let output = take(&array, &indices).unwrap();

    let boolean = BooleanArray::from(&[None, Some(true), Some(false), None, Some(true)]);
    let int = Int32Array::from(&[None, Some(31), Some(28), None, Some(42)]);
    let validity = vec![false, true, true, false, true]
        .into_iter()
        .collect::<MutableBitmap>()
        .into();
    let expected = StructArray::from_data(
        array.data_type().clone(),
        vec![
            Arc::new(boolean) as Arc<dyn Array>,
            Arc::new(int) as Arc<dyn Array>,
        ],
        validity,
    );
    assert_eq!(expected, output.as_ref());
}

#[test]
fn consistency() {
    use arrow2::array::new_null_array;
    use arrow2::datatypes::DataType::*;
    use arrow2::datatypes::TimeUnit;

    let datatypes = vec![
        Null,
        Boolean,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Int8,
        Int16,
        Int32,
        Int64,
        Float32,
        Float64,
        Timestamp(TimeUnit::Second, None),
        Timestamp(TimeUnit::Millisecond, None),
        Timestamp(TimeUnit::Microsecond, None),
        Timestamp(TimeUnit::Nanosecond, None),
        Time64(TimeUnit::Microsecond),
        Time64(TimeUnit::Nanosecond),
        Interval(IntervalUnit::DayTime),
        Interval(IntervalUnit::YearMonth),
        Date32,
        Time32(TimeUnit::Second),
        Time32(TimeUnit::Millisecond),
        Date64,
        Utf8,
        LargeUtf8,
        Binary,
        LargeBinary,
        Duration(TimeUnit::Second),
        Duration(TimeUnit::Millisecond),
        Duration(TimeUnit::Microsecond),
        Duration(TimeUnit::Nanosecond),
    ];

    datatypes.into_iter().for_each(|d1| {
        let array = new_null_array(d1.clone(), 10);
        let indices = Int32Array::from(&[Some(1), Some(2), None, Some(3)]);
        if can_take(&d1) {
            assert!(take(array.as_ref(), &indices).is_ok());
        } else {
            assert!(take(array.as_ref(), &indices).is_err());
        }
    });
}

#[test]
fn empty() {
    let indices = Int32Array::from_slice(&[]);
    let values = BooleanArray::from(vec![Some(true), Some(false)]);
    let a = take(&values, &indices).unwrap();
    assert_eq!(a.len(), 0)
}

#[test]
fn unsigned_take() {
    let indices = UInt32Array::from_slice(&[]);
    let values = BooleanArray::from(vec![Some(true), Some(false)]);
    let a = take(&values, &indices).unwrap();
    assert_eq!(a.len(), 0)
}
