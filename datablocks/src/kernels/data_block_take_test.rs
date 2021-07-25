
use common_exception::Result;

use crate::*;
use arrow2::api::prelude::DataType;

#[test]
fn test_data_block_take() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Utf8, false),
    ]);

    let raw = DataBlock::create_by_array(schema.clone(), vec![
        Series::new(vec![1i64, 2, 3]),
        Series::new(vec!["b1", "b2", "b3"]),
    ]);

    let take = DataBlock::block_take_by_indices(&raw, &[], &[0, 2])?;
    assert_eq!(raw.schema(), take.schema());

    let expected = vec![
        "+---+----+",
        "| a | b  |",
        "+---+----+",
        "| 1 | b1 |",
        "| 3 | b3 |",
        "+---+----+",
    ];
    crate::assert_blocks_eq(expected, &[take]);

    Ok(())
}
