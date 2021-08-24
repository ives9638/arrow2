use std::fs::File;
use std::sync::Arc;

use arrow2::error::Result;
use arrow2::io::parquet::read;
use parquet2::read::PageFilter;
use arrow2::io::parquet::read::{DataPageHeader, ColumnDescriptor};

fn main() -> Result<()> {
   // use std::env;
   // let args: Vec<String> = env::args().collect();

   // let file_path = &args[1];

    let reader = File::open("/media/lidn/data/aaa/test.parquet")?;

    let page_filter =   Arc::new(|x:&ColumnDescriptor, y:&DataPageHeader| {
        if x.name() == "c1" {
            true
        }
        else { true }
    });

    let reader = read::RecordReader::try_new(reader, None, None, Arc::new(|_, _| true), Some(page_filter))?;

    for maybe_batch in reader {
        let batch = maybe_batch?;
        println!("{:?}", batch);
    }
    Ok(())
}
