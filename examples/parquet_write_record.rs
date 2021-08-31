use std::fs::File;
use std::sync::Arc;

use arrow2::{
    array::{Array, Int32Array},
    datatypes::{Field, Schema},
    error::Result,
    io::parquet::write::{
        write_file, Compression, Encoding, RowGroupIterator, Version, WriteOptions,
    },
    record_batch::RecordBatch,
};

fn write_batch(path: &str, batch: RecordBatch) -> Result<()> {
    let schema = batch.schema().clone();

    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V2,
    };

    let iter = vec![Ok(batch)];

    let row_groups =
        RowGroupIterator::try_new(iter.into_iter(), &schema, options, vec![Encoding::Plain,Encoding::Plain])?;

    // Create a new empty file
    let mut file = File::create(path)?;

    // Write the file. Note that, at present, any error results in a corrupted file.
    let parquet_schema = row_groups.parquet_schema().clone();
    let _ = write_file(
        &mut file,
        row_groups,
        &schema,
        parquet_schema,
        options,
        None,
    )?;
    Ok(())
}

fn main() -> Result<()> {
    let array1 = Int32Array::from(&[
        Some(0),
        Some(1),
        Some(2),
       None,
        Some(4),
        Some(5),
        Some(6),
    ]);
    let array = Int32Array::from(&[
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ]);
    let field = Field::new("c1", array.data_type().clone(), true);
    let field1 = Field::new("c2", array1.data_type().clone(), true);
    let schema = Schema::new(vec![field,field1]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array),Arc::new(array1)])?;

    write_batch("/media/lidn/data/aaa/test.parquet", batch)
}
