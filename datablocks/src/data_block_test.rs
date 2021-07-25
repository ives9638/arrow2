use crate::DataBlock;
use arrow2::api::prelude::{Arc, DataColumn, DataSchemaRef, DataType, Schema};
use arrow2::datatypes::Field;
use common_exception::Result;


#[test]
fn test_data_block() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

    let block = DataBlock::create_by_array(
        schema.clone(),
        vec![DataColumn::from_array( vec![1.0, 2.2, 3.2].into_iter())],
    );
    assert_eq!(&schema, block.schema());

    assert_eq!(3, block.num_rows());
    assert_eq!(1, block.num_columns());
    assert_eq!(3, block.try_column_by_name("a")?.len());
    assert_eq!(3, block.column(0).len());

    assert_eq!(true, block.column_by_name("a").is_some());
    assert_eq!(true, block.column_by_name("a_not_found").is_none());

    Ok(())
}
