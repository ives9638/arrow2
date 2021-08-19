// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[macro_use]
extern crate criterion;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::distributions::Uniform;
use rand::Rng;

use arrow2::array::*;
use arrow2::compute::cast;
use arrow2::datatypes::*;
use arrow2::util::bench_util::*;
use arrow2::api::list::Islist;

fn build_utf8_date_array(size: usize, with_nulls: bool) -> Utf8Array<i32> {
    use chrono::NaiveDate;

    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();
    let range = Uniform::new(0, 737776);

    (0..size)
        .map(|_| {
            if with_nulls && rng.gen::<f32>() > 0.8 {
                None
            } else {
                Some(
                    NaiveDate::from_num_days_from_ce(rng.sample(range))
                        .format("%Y-%m-%d")
                        .to_string(),
                )
            }
        })
        .collect()
}

fn build_utf8_date_time_array(size: usize, with_nulls: bool) -> Utf8Array<i32> {
    use chrono::NaiveDateTime;

    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();
    let range = Uniform::new(0, 1608071414123);

    (0..size)
        .map(|_| {
            if with_nulls && rng.gen::<f32>() > 0.8 {
                None
            } else {
                Some(
                    NaiveDateTime::from_timestamp(rng.sample(range), 0)
                        .format("%Y-%m-%dT%H:%M:%S")
                        .to_string(),
                )
            }
        })
        .collect()
}

// cast array from specified primitive array type to desired data type
fn cast_array(array: &dyn Array, to_type: DataType) {
    criterion::black_box(cast::cast(array, &to_type).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let size = 5120;
    let i32_array = create_primitive_array::<i32>(size, DataType::Int32, 0.1);

    let list_i32_array = arrow2::api::List::from(i32_array.clone());

    let i64_array = create_primitive_array::<i64>(size, DataType::Int64, 0.1);
    let f32_array = create_primitive_array::<f32>(size, DataType::Float32, 0.1);
    let f32_utf8_array = cast::cast(&f32_array, &DataType::Utf8).unwrap();

    let f64_array = create_primitive_array::<f64>(size, DataType::Float64, 0.1);
    let date64_array = create_primitive_array::<i64>(size, DataType::Date64, 0.1);
    let date32_array = create_primitive_array::<i32>(size, DataType::Date32, 0.1);
    let time32s_array =
        create_primitive_array::<i32>(size, DataType::Time32(TimeUnit::Second), 0.1);
    let time64ns_array =
        create_primitive_array::<i64>(size, DataType::Time64(TimeUnit::Nanosecond), 0.1);
    let time_ns_array =
        create_primitive_array::<i64>(size, DataType::Timestamp(TimeUnit::Nanosecond, None), 0.1);
    let time_ms_array =
        create_primitive_array::<i64>(size, DataType::Timestamp(TimeUnit::Millisecond, None), 0.1);
    let utf8_date_array = build_utf8_date_array(512, true);
    let utf8_date_time_array = build_utf8_date_time_array(512, true);

    c.bench_function("cast int32 to int32 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Int32))
    });
    c.bench_function("cast int32 to uint32 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::UInt32))
    });


    c.bench_function("cast int32 to float32 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Float32))
    });
    c.bench_function("cast int32 to float64 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Float64))
    });
    c.bench_function("cast int32 to int64 512", |b| {
        b.iter(|| cast_array(&i32_array, DataType::Int64))
    });
    c.bench_function("cast float32 to int32 512", |b| {
        b.iter(|| cast_array(&f32_array, DataType::Int32))
    });
    c.bench_function("cast float64 to float32 512", |b| {
        b.iter(|| cast_array(&f64_array, DataType::Float32))
    });
    c.bench_function("cast float64 to uint64 512", |b| {
        b.iter(|| cast_array(&f64_array, DataType::UInt64))
    });
    c.bench_function("cast int64 to int32 512", |b| {
        b.iter(|| cast_array(&i64_array, DataType::Int32))
    });
    c.bench_function("cast date64 to date32 512", |b| {
        b.iter(|| cast_array(&date64_array, DataType::Date32))
    });
    c.bench_function("cast date32 to date64 512", |b| {
        b.iter(|| cast_array(&date32_array, DataType::Date64))
    });
    c.bench_function("cast time32s to time32ms 512", |b| {
        b.iter(|| cast_array(&time32s_array, DataType::Time32(TimeUnit::Millisecond)))
    });
    c.bench_function("cast time32s to time64us 512", |b| {
        b.iter(|| cast_array(&time32s_array, DataType::Time64(TimeUnit::Microsecond)))
    });
    c.bench_function("cast time64ns to time32s 512", |b| {
        b.iter(|| cast_array(&time64ns_array, DataType::Time32(TimeUnit::Second)))
    });
    c.bench_function("cast timestamp_ns to timestamp_s 512", |b| {
        b.iter(|| {
            cast_array(
                &time_ns_array,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )
        })
    });
    c.bench_function("cast timestamp_ms to timestamp_ns 512", |b| {
        b.iter(|| {
            cast_array(
                &time_ms_array,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )
        })
    });
    c.bench_function("cast utf8 to f32", |b| {
        b.iter(|| cast_array(f32_utf8_array.as_ref(), DataType::Float32))
    });
    c.bench_function("cast i64 to string 512", |b| {
        b.iter(|| cast_array(&i64_array, DataType::Utf8))
    });
    c.bench_function("cast f32 to string 512", |b| {
        b.iter(|| cast_array(&f32_array, DataType::Utf8))
    });

    c.bench_function("cast timestamp_ms to i64 512", |b| {
        b.iter(|| cast_array(&time_ms_array, DataType::Int64))
    });
    c.bench_function("cast utf8 to date32 512", |b| {
        b.iter(|| cast_array(&utf8_date_array, DataType::Date32))
    });
    c.bench_function("cast utf8 to date64 512", |b| {
        b.iter(|| cast_array(&utf8_date_time_array, DataType::Date64))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
