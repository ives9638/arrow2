use crate::api::IValue::types::*;

macro_rules! Native_for_IValue {
    ($ca: ident,$cb:ident ) => {
        impl Into<Option<$ca>> for IValue {
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

Native_for_IValue!(i32, I32);
Native_for_IValue!(i64, I64);
Native_for_IValue!(i8, I8);
Native_for_IValue!(i16, I16);
Native_for_IValue!(bool, IBool);

impl Into<String> for IValue {
    fn into(self) -> String {
        let q = self.0.as_ref().as_any().downcast_ref::<IStr>();
        q.unwrap().value().unwrap().to_string()
    }
}
