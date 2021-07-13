use crate::api::types::{IStr, IValue, I32};

impl Into<i32> for IValue {
    fn into(self) -> i32 {
        let q = self.0.as_ref().as_any().downcast_ref::<I32>();
        q.unwrap().value() as i32
    }
}
impl Into<String> for IValue {
    fn into(self) -> String {
        let q = self.0.as_ref().as_any().downcast_ref::<IStr>();
        q.unwrap().value().to_string()
    }
}
