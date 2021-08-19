use crate::{
    datatypes::DataType,
    types::{NativeType, NaturalDataType},
};

use super::Scalar;

#[derive(Debug, Clone)]
pub struct PrimitiveScalar<T: NativeType> {
    // Not Option<T> because this offers a stabler pointer offset on the struct
    value: T,
    is_valid: bool,
    data_type: DataType,
}

impl<T: NativeType> PartialEq for PrimitiveScalar<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type
            && self.is_valid == other.is_valid
            && ((!self.is_valid) | (self.value == other.value))
    }
}

impl<T: NativeType> PrimitiveScalar<T> {
    #[inline]
    pub fn new(data_type: DataType, v: Option<T>) -> Self {
        let is_valid = v.is_some();
        Self {
            value: v.unwrap_or_default(),
            is_valid,
            data_type,
        }
    }

    #[inline]
    pub fn value(&self) -> T {
        self.value
    }

    /// Returns a new `PrimitiveScalar` with the same value but different [`DataType`]
    /// # Panic
    /// This function panics if the `data_type` is not valid for self's physical type `T`.
    pub fn to(self, data_type: DataType) -> Self {
        let v = if self.is_valid {
            Some(self.value)
        } else {
            None
        };
        Self::new(data_type, v)
    }
}

impl<T: NativeType + NaturalDataType> From<Option<T>> for PrimitiveScalar<T> {
    #[inline]
    fn from(v: Option<T>) -> Self {
        Self::new(T::DATA_TYPE, v)
    }
}

impl<T: NativeType> Scalar for PrimitiveScalar<T> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.is_valid
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::eq_op)]
    #[test]
    fn equal() {
        let a = PrimitiveScalar::from(Some(2i32));
        let b = PrimitiveScalar::<i32>::from(None);
        assert_eq!(a, a);
        assert_eq!(b, b);
        assert!(a != b);
        let b = PrimitiveScalar::<i32>::from(Some(1i32));
        assert!(a != b);
        assert_eq!(b, b);
    }

    #[test]
    fn basics() {
        let a = PrimitiveScalar::from(Some(2i32));

        assert_eq!(a.value(), 2i32);
        assert_eq!(a.data_type(), &DataType::Int32);
        assert!(a.is_valid());

        let a = a.to(DataType::Date32);
        assert_eq!(a.data_type(), &DataType::Date32);

        let a = PrimitiveScalar::<i32>::from(None);

        assert_eq!(a.data_type(), &DataType::Int32);
        assert!(!a.is_valid());

        let a = a.to(DataType::Date32);
        assert_eq!(a.data_type(), &DataType::Date32);
    }
}
