use crate::api::scalar::DataValue;
use crate::io::ipc::gen::Schema::Bool;
use crate::scalar::PrimitiveScalar;
use crate::scalar::*;
use crate::api::prelude::DataType;
use crate::datatypes::DataType::*;
macro_rules! Native_for_DataValue {
    ($ca: ident,$cb:ident ,$dt :ident ) => {
        impl Into<Option<$ca>> for DataValue {
            fn into(self) -> Option<$ca> {
                let q = self.0.as_ref().as_any().downcast_ref::<$cb>();
                match q.unwrap().value() {
                    Some(v) => Some(v) as Option<$ca>,
                    None => None as Option<$ca>,
                }
            }
        }
        impl Into<DataValue> for Option<$ca> {
            fn into(self) -> DataValue {
                $cb::new( $dt,self.clone()).into_value()
            }
        }
    };
}

Native_for_DataValue!(i8, Int8Scalar,Int8);
Native_for_DataValue!(i16, Int16Scalar,Int16);
Native_for_DataValue!(i32, Int32Scalar ,Int32);
Native_for_DataValue!(i64, Int64Scalar ,Int64 );

Native_for_DataValue!(f32, Float32Scalar, Float32);
Native_for_DataValue!(f64, Float64Scalar ,Float64);

Native_for_DataValue!(u8, UInt8Scalar , UInt8);
Native_for_DataValue!(u16, UInt16Scalar,UInt16);
Native_for_DataValue!(u32, UInt32Scalar , UInt32);
Native_for_DataValue!(u64, UInt64Scalar ,UInt64);

impl Into<DataValue> for Option<bool> {
    fn into(self) -> DataValue {
        match self {
            None => {
                BooleanScalar::new(None).into_value()
            }
            Some(v) => {
                BooleanScalar::new(Some(v)).into_value()
            }
        }
    }
}
impl Into<DataValue> for Option<&str> {
    fn into(self) -> DataValue {
        match self {
            None => {
                Utf8Scalar::<i32>::new(None).into_value()
            }
            Some(v) => {
                Utf8Scalar::<i32>::new(Some(v)).into_value()
            }
        }
    }
}
impl Into<DataValue> for &str {
    fn into(self) -> DataValue {
        if self.is_empty() {
            Utf8Scalar::<i32>::new(None).into_value()
        } else {
            Utf8Scalar::<i32>::new(Some(self)).into_value()
        }

    }
}
impl Into<DataValue> for bool {
    fn into(self) -> DataValue {
        BooleanScalar::new(Some(self.clone())).into_value()
    }
}

impl Into<String> for DataValue {
    fn into(self) -> String {
        let q = self.0.as_ref().as_any().downcast_ref::<Utf8Scalar<i32>>();
        q.unwrap().value().unwrap().to_string()
    }
}
