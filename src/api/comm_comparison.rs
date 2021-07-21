
use crate::array::Array;
use crate::array::Float32Array;
use crate::array::Float64Array;
use crate::array::Int128Array;
use crate::array::Int16Array;
use crate::array::Int32Array;
use crate::array::Int64Array;
use crate::array::Int8Array;
use crate::array::UInt16Array;
use crate::array::UInt32Array;
use crate::array::UInt64Array;
use crate::array::UInt8Array;
use crate::array::Utf8Array;
use crate::compute::comparison::*;
use crate::datatypes::DataType;
use crate::datatypes::IntervalUnit;
use crate::error::*;
use crate::scalar::BooleanScalar;
use crate::scalar::Float32Scalar;
use crate::scalar::Float64Scalar;
use crate::scalar::Int128Scalar;
use crate::scalar::Int16Scalar;
use crate::scalar::Int32Scalar;
use crate::scalar::Int64Scalar;
use crate::scalar::Int8Scalar;
use crate::scalar::PrimitiveScalar;
use crate::scalar::UInt16Scalar;
use crate::scalar::UInt32Scalar;
use crate::scalar::UInt64Scalar;
use crate::scalar::UInt8Scalar;
use crate::scalar::Utf8Scalar;
use crate::api::Ivalue::Ivalue;

pub fn comparison_array(lhs: &dyn Array, op: Operator, rhs: &dyn Array) -> Result<Box<dyn Array>> {
    if !can_compare(lhs.data_type()) {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type comparison not supported {:?}  ",
            lhs.data_type()
        )));
    }
    compare(lhs, rhs, op)
        .map(Box::new)
        .map(|x| x as Box<dyn Array>)
}
pub fn comparison_scalar(
    lhs: &dyn Array,
    operator: Operator,
    rhs: &Ivalue,
) -> Result<Box<dyn Array>> {
    let data_type = lhs.data_type();
    if data_type != rhs.0.data_type() {
        return Err(ArrowError::NotYetImplemented(
            "Comparison is only supported for arrays of the same logical type".to_string(),
        ));
    }
    match data_type {
        DataType::Boolean => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<BooleanScalar>().unwrap();
            boolean_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        DataType::Int8 => {
            let lhs = lhs.as_any().downcast_ref::<Int8Array>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<Int8Scalar>().unwrap();
            primitive_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        DataType::Int16 => {
            let lhs = lhs.as_any().downcast_ref::<Int16Array>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<Int16Scalar>().unwrap();
            primitive_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            let lhs = lhs.as_any().downcast_ref::<Int32Array>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<Int32Scalar>().unwrap();
            primitive_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        DataType::Int64
        | DataType::Timestamp(_, None)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_) => {
            let lhs = lhs.as_any().downcast_ref::<Int64Array>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<Int64Scalar>().unwrap();
            primitive_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        DataType::UInt8 => {
            let lhs = lhs.as_any().downcast_ref::<UInt8Array>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<UInt8Scalar>().unwrap();
            primitive_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        DataType::UInt16 => {
            let lhs = lhs.as_any().downcast_ref::<UInt16Array>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<UInt16Scalar>().unwrap();
            primitive_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        DataType::UInt32 => {
            let lhs = lhs.as_any().downcast_ref::<UInt32Array>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<UInt32Scalar>().unwrap();
            primitive_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        DataType::UInt64 => {
            let lhs = lhs.as_any().downcast_ref::<UInt64Array>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<UInt64Scalar>().unwrap();
            primitive_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        DataType::Float16 => unreachable!(),
        DataType::Float32 => {
            let lhs = lhs.as_any().downcast_ref::<Float32Array>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<Float32Scalar>().unwrap();
            primitive_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        DataType::Float64 => {
            let lhs = lhs.as_any().downcast_ref::<Float64Array>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<Float64Scalar>().unwrap();
            primitive_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        DataType::Interval(IntervalUnit::DayTime) => unreachable!(),
        DataType::Utf8 => {
            let lhs = lhs.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<Utf8Scalar<i32>>().unwrap();
            Ok(Box::new(utf8_compare_scalar(lhs, rhs.value().unwrap(), operator)))
        }
        DataType::LargeUtf8 => {
            let lhs = lhs.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<Utf8Scalar<i64>>().unwrap();
            Ok(Box::new(utf8_compare_scalar(lhs, rhs.value().unwrap(), operator)))
        }
        DataType::Decimal(_, _) => {
            let lhs = lhs.as_any().downcast_ref::<Int128Array>().unwrap();
            let rhs = rhs.0.as_any().downcast_ref::<Int128Scalar>().unwrap();
            primitive_compare_scalar(lhs, rhs.value().unwrap(), operator)
                .map(Box::new)
                .map(|x| x as Box<dyn Array>)
        }
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Comparison between {:?} is not supported",
            data_type
        ))),
    }
}
