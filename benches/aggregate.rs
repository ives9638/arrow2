use arrow2::api::compute::cast::list::Islist;
use arrow2::array::*;
use arrow2::util::bench_util::*;
use arrow2::{compute::aggregate::*, datatypes::DataType};
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_sum(arr_a: &PrimitiveArray<i32>) {
    sum(criterion::black_box(arr_a)).unwrap();
}

fn bench_min(arr_a: &PrimitiveArray<i32>) {
    min_primitive(criterion::black_box(arr_a)).unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size) * 10000;
        let arr_a = create_primitive_array::<i32>(size, DataType::Int32, 0.0);
        let list_a = arrow2::api::types::list::List::from(arr_a.clone());
        let mut packed_value = 0_u128;
        c.bench_function(&format!("sum_list 2^{} f32", log2_size), |b| {
            b.iter(|| {
                let binArray =

                    arrow2::api::types::list::List::pack_to_u128(criterion::black_box(&list_a),0_u128, 1);


            })
        });

        c.bench_function(&format!("sum 2^{} f32", log2_size), |b| {
            b.iter(|| bench_sum(&arr_a))
        });
        c.bench_function(&format!("min 2^{} f32", log2_size), |b| {
            b.iter(|| bench_min(&arr_a))
        });

        let arr_a = create_primitive_array::<i32>(size, DataType::Int32, 0.1);
        let list_a = arrow2::api::types::list::List::from(arr_a.clone());
        c.bench_function(&format!("sum null 2^{} f32", log2_size), |b| {
            b.iter(|| criterion::black_box(&list_a).sum())
        });

        c.bench_function(&format!("sum null 2^{} f32", log2_size), |b| {
            b.iter(|| bench_sum(&arr_a))
        });

        c.bench_function(&format!("min null 2^{} f32", log2_size), |b| {
            b.iter(|| bench_min(&arr_a))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
