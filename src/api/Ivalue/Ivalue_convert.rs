use crate::api::Ivalue::Ivalue;
use crate::scalar::{Int32Scalar, Int64Scalar, Int8Scalar, Int16Scalar, BooleanScalar, Utf8Scalar};
use crate::scalar::PrimitiveScalar;
macro_rules! Native_for_Ivalue {
    ($ca: ident,$cb:ident ) => {
        impl Into<Option<$ca>> for Ivalue {
            fn into(self) -> Option<$ca> {
                let q = self.0.as_ref().as_any().downcast_ref::<$cb>();
                match q.unwrap().value() {
                    Some(v) => Some(v) as Option<$ca>,
                    None => None as Option<$ca>,
                }
            }
        }
    };
}

Native_for_Ivalue!(i32, Int32Scalar);
Native_for_Ivalue!(i64, Int64Scalar);
Native_for_Ivalue!(i8, Int8Scalar);
Native_for_Ivalue!(i16, Int16Scalar);
Native_for_Ivalue!(bool, BooleanScalar);

impl Into<String> for Ivalue {
    fn into(self) -> String {
        let q = self.0.as_ref().as_any().downcast_ref::<Utf8Scalar<i32>>();
        q.unwrap().value().unwrap().to_string()
    }
}
