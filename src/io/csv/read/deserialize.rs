use std::sync::Arc;

use chrono::Datelike;
use csv::ByteRecord;

use crate::{
    array::*,
    datatypes::*,
    error::{ArrowError, Result},
    record_batch::RecordBatch,
    temporal_conversions,
    types::{NativeType, NaturalDataType},
};

use super::infer_schema::RFC3339;

fn deserialize_primitive<T, F>(
    rows: &[ByteRecord],
    column: usize,
    datatype: DataType,
    op: F,
) -> Arc<dyn Array>
where
    T: NativeType + NaturalDataType + lexical_core::FromLexical,
    F: Fn(&[u8]) -> Option<T>,
{
    let iter = rows.iter().map(|row| match row.get(column) {
        Some(bytes) => {
            if bytes.is_empty() {
                return None;
            }
            op(bytes)
        }
        None => None,
    });
    Arc::new(PrimitiveArray::<T>::from_trusted_len_iter(iter).to(datatype))
}

fn deserialize_boolean<F>(rows: &[ByteRecord], column: usize, op: F) -> Arc<dyn Array>
where
    F: Fn(&[u8]) -> Option<bool>,
{
    let iter = rows.iter().map(|row| match row.get(column) {
        Some(bytes) => {
            if bytes.is_empty() {
                return None;
            }
            op(bytes)
        }
        None => None,
    });
    Arc::new(BooleanArray::from_trusted_len_iter(iter))
}

fn deserialize_utf8<O: Offset>(rows: &[ByteRecord], column: usize) -> Arc<dyn Array> {
    let iter = rows.iter().map(|row| match row.get(column) {
        Some(bytes) => simdutf8::basic::from_utf8(bytes).ok(),
        None => None,
    });
    Arc::new(Utf8Array::<O>::from_trusted_len_iter(iter))
}

fn deserialize_binary<O: Offset>(rows: &[ByteRecord], column: usize) -> Arc<dyn Array> {
    let iter = rows.iter().map(|row| row.get(column));
    Arc::new(BinaryArray::<O>::from_trusted_len_iter(iter))
}

#[inline]
fn deserialize_datetime<T: chrono::TimeZone>(string: &str, tz: &T) -> Option<i64> {
    let mut parsed = chrono::format::Parsed::new();
    let fmt = chrono::format::StrftimeItems::new(RFC3339);
    if chrono::format::parse(&mut parsed, string, fmt).is_ok() {
        parsed
            .to_datetime()
            .map(|x| x.naive_utc())
            .map(|x| tz.from_utc_datetime(&x))
            .map(|x| x.timestamp_nanos())
            .ok()
    } else {
        None
    }
}

/// Deserializes `column` of `rows` into an [`Array`] of [`DataType`] `datatype`.
pub fn deserialize_column(
    rows: &[ByteRecord],
    column: usize,
    datatype: DataType,
    _line_number: usize,
) -> Result<Arc<dyn Array>> {
    use DataType::*;
    Ok(match datatype {
        Boolean => deserialize_boolean(rows, column, |bytes| {
            if bytes.eq_ignore_ascii_case(b"false") {
                Some(false)
            } else if bytes.eq_ignore_ascii_case(b"true") {
                Some(true)
            } else {
                None
            }
        }),
        Int8 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<i8>(bytes).ok()
        }),
        Int16 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<i16>(bytes).ok()
        }),
        Int32 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<i32>(bytes).ok()
        }),
        Int64 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<i64>(bytes).ok()
        }),
        UInt8 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<u8>(bytes).ok()
        }),
        UInt16 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<u16>(bytes).ok()
        }),
        UInt32 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<u32>(bytes).ok()
        }),
        UInt64 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<u64>(bytes).ok()
        }),
        Float32 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<f32>(bytes).ok()
        }),
        Float64 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<f64>(bytes).ok()
        }),
        Date32 => deserialize_primitive(rows, column, datatype, |bytes| {
            simdutf8::basic::from_utf8(bytes)
                .ok()
                .and_then(|x| x.parse::<chrono::NaiveDate>().ok())
                .map(|x| x.num_days_from_ce() - temporal_conversions::EPOCH_DAYS_FROM_CE)
        }),
        Date64 => deserialize_primitive(rows, column, datatype, |bytes| {
            simdutf8::basic::from_utf8(bytes)
                .ok()
                .and_then(|x| x.parse::<chrono::NaiveDateTime>().ok())
                .map(|x| x.timestamp_millis())
        }),
        Timestamp(TimeUnit::Nanosecond, None) => {
            deserialize_primitive(rows, column, datatype, |bytes| {
                simdutf8::basic::from_utf8(bytes)
                    .ok()
                    .and_then(|x| x.parse::<chrono::NaiveDateTime>().ok())
                    .map(|x| x.timestamp_nanos())
            })
        }
        Timestamp(TimeUnit::Microsecond, None) => {
            deserialize_primitive(rows, column, datatype, |bytes| {
                simdutf8::basic::from_utf8(bytes)
                    .ok()
                    .and_then(|x| x.parse::<chrono::NaiveDateTime>().ok())
                    .map(|x| x.timestamp_nanos() / 1000)
            })
        }
        Timestamp(time_unit, None) => deserialize_primitive(rows, column, datatype, |bytes| {
            simdutf8::basic::from_utf8(bytes)
                .ok()
                .and_then(|x| x.parse::<chrono::NaiveDateTime>().ok())
                .map(|x| x.timestamp_nanos())
                .map(|x| match time_unit {
                    TimeUnit::Second => x / 1_000_000_000,
                    TimeUnit::Millisecond => x / 1_000_000,
                    TimeUnit::Microsecond => x / 1_000,
                    TimeUnit::Nanosecond => x,
                })
        }),
        Timestamp(time_unit, Some(ref tz)) => {
            let tz = temporal_conversions::parse_offset(tz)?;
            deserialize_primitive(rows, column, datatype, |bytes| {
                simdutf8::basic::from_utf8(bytes)
                    .ok()
                    .and_then(|x| deserialize_datetime(x, &tz))
                    .map(|x| match time_unit {
                        TimeUnit::Second => x / 1_000_000_000,
                        TimeUnit::Millisecond => x / 1_000_000,
                        TimeUnit::Microsecond => x / 1_000,
                        TimeUnit::Nanosecond => x,
                    })
            })
        }
        Utf8 => deserialize_utf8::<i32>(rows, column),
        LargeUtf8 => deserialize_utf8::<i64>(rows, column),
        Binary => deserialize_binary::<i32>(rows, column),
        LargeBinary => deserialize_binary::<i64>(rows, column),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Deserializing type \"{:?}\" is not implemented",
                other
            )))
        }
    })
}

/// Deserializes rows [`ByteRecord`] into a [`RecordBatch`].
/// Note that this is a convenience function: column deserialization
///is trivially parallelizable (e.g. rayon).
pub fn deserialize_batch<F>(
    rows: &[ByteRecord],
    fields: &[Field],
    projection: Option<&[usize]>,
    line_number: usize,
    deserialize_column: F,
) -> Result<RecordBatch>
where
    F: Fn(&[ByteRecord], usize, DataType, usize) -> Result<Arc<dyn Array>>,
{
    let projection: Vec<usize> = match projection {
        Some(v) => v.to_vec(),
        None => fields.iter().enumerate().map(|(i, _)| i).collect(),
    };
    let projected_fields: Vec<Field> = projection.iter().map(|i| fields[*i].clone()).collect();

    let schema = Arc::new(Schema::new(projected_fields));

    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let columns = projection
        .iter()
        .map(|column| {
            let column = *column;
            let field = &fields[column];
            let data_type = field.data_type();
            deserialize_column(rows, column, data_type.clone(), line_number)
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(schema, columns)
}
