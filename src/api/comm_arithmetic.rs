use crate::api::IValue::IValue;
use crate::array::Array;
use crate::compute::arithmetics::Operator;
use crate::compute::arithmetics::*;
use crate::compute::cast::can_cast_types;
use crate::compute::cast::cast;
use crate::datatypes::DataType;
use crate::datatypes::DataType::*;
use crate::datatypes::TimeUnit;
use crate::error::*;
use crate::scalar::PrimitiveScalar;
use crate::scalar::Scalar;

pub fn arithmetic_array(lhs: &dyn Array, op: Operator, rhs: &dyn Array) -> Result<Box<dyn Array>> {
    arithmetic(lhs, op, rhs)
}
macro_rules! primitive_scalar {
    ($lhs: expr, $rhs: expr, $op: expr, $array_type: ty) => {{
        let res_lhs = $lhs.as_any().downcast_ref().unwrap();
        let res_rhs = $rhs
            .0
            .as_any()
            .downcast_ref::<PrimitiveScalar<$array_type>>()
            .unwrap();
        arithmetic_primitive_scalar::<$array_type>(res_lhs, $op, &res_rhs.value().unwrap())
            .map(Box::new)
            .map(|x| x as Box<dyn Array>)
    }};
}
pub fn arithmetic_scalar(lhs: &dyn Array, op: Operator, rhs: &IValue) -> Result<Box<dyn Array>> {
    use Operator::*;
    match (lhs.data_type(), op, rhs.0.data_type()) {
        (Int8, _, Int8) => {
            primitive_scalar!(lhs, rhs, op, i8)
        }
        (Int16, _, Int16) => primitive_scalar!(lhs, rhs, op, i16),
        (Int32, _, Int32) => primitive_scalar!(lhs, rhs, op, i32),
        (Int64, _, Int64) | (Duration(_), _, Duration(_)) => {
            primitive_scalar!(lhs, rhs, op, i64)
        }
        (UInt8, _, UInt8) => primitive_scalar!(lhs, rhs, op, u8),
        (UInt16, _, UInt16) => primitive_scalar!(lhs, rhs, op, u16),
        (UInt32, _, UInt32) => primitive_scalar!(lhs, rhs, op, u32),
        (UInt64, _, UInt64) => primitive_scalar!(lhs, rhs, op, u64),
        (Float32, _, Float32) => primitive_scalar!(lhs, rhs, op, f32),
        (Float64, _, Float64) => primitive_scalar!(lhs, rhs, op, f64),
        (Decimal(_, _), _, Decimal(_, _)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs
                .0
                .as_any()
                .downcast_ref::<PrimitiveScalar<i128>>()
                .unwrap();
            arithmetic_primitive_scalar::<i128>(lhs, op, &rhs.value().unwrap())
                .map(|x| Box::new(x) as Box<dyn Array>)
        }
        (Time32(TimeUnit::Second), Add, Duration(_))
        | (Time32(TimeUnit::Millisecond), Add, Duration(_))
        | (Date32, Add, Duration(_)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            time::add_duration::<i32>(
                lhs,
                rhs.0
                    .to_boxed_array(lhs.len())
                    .as_any()
                    .downcast_ref()
                    .unwrap(),
            )
            .map(|x| Box::new(x) as Box<dyn Array>)
        }
        (Time32(TimeUnit::Second), Subtract, Duration(_))
        | (Time32(TimeUnit::Millisecond), Subtract, Duration(_))
        | (Date32, Subtract, Duration(_)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            time::subtract_duration::<i32>(
                lhs,
                rhs.0
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<i64>>()
                    .unwrap()
                    .to_boxed_array(lhs.len())
                    .as_any()
                    .downcast_ref()
                    .unwrap(),
            )
            .map(|x| Box::new(x) as Box<dyn Array>)
        }
        (Time64(TimeUnit::Microsecond), Add, Duration(_))
        | (Time64(TimeUnit::Nanosecond), Add, Duration(_))
        | (Date64, Add, Duration(_))
        | (Timestamp(_, _), Add, Duration(_)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            time::add_duration::<i64>(
                lhs,
                rhs.0
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<i64>>()
                    .unwrap()
                    .to_boxed_array(lhs.len())
                    .as_any()
                    .downcast_ref()
                    .unwrap(),
            )
            .map(|x| Box::new(x) as Box<dyn Array>)
        }
        (Time64(TimeUnit::Microsecond), Subtract, Duration(_))
        | (Time64(TimeUnit::Nanosecond), Subtract, Duration(_))
        | (Date64, Subtract, Duration(_))
        | (Timestamp(_, _), Subtract, Duration(_)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            time::subtract_duration::<i64>(
                lhs,
                rhs.0
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<i64>>()
                    .unwrap()
                    .to_boxed_array(lhs.len())
                    .as_any()
                    .downcast_ref()
                    .unwrap(),
            )
            .map(|x| Box::new(x) as Box<dyn Array>)
        }
        (Timestamp(_, None), Subtract, Timestamp(_, None)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            time::subtract_timestamps(
                lhs,
                rhs.0
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<i64>>()
                    .unwrap()
                    .to_boxed_array(lhs.len())
                    .as_any()
                    .downcast_ref()
                    .unwrap(),
            )
            .map(|x| Box::new(x) as Box<dyn Array>)
        }
        (lhs, op, rhs) => Err(ArrowError::NotYetImplemented(format!(
            "Arithmetics of ({:?}, {:?}, {:?}) is not supported",
            lhs, op, rhs
        ))),
    }
}

pub fn arithmetic_type_cast(lhs: &dyn Array, to_type: &DataType) -> Result<Box<dyn Array>> {
    if !can_cast_types(lhs.data_type(), &to_type) {
        return Err(ArrowError::NotYetImplemented(format!(
            "Type cast not supported {:?} to {:?}  ",
            lhs.data_type(),
            to_type
        )));
    }
    cast(lhs, to_type)
}
