
use common_exception::Result;

use crate::*;

#[test]
fn test_data_block_scatter() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Float64, false),
    ]);

    let raw = DataBlock::create(schema.clone(), vec![
        Series::new(vec![1i64, 2, 3]).into(),
        Series::new(vec![1.0f64, 2., 3.]).into(),
    ]);

    let indices = DataColumn::Array(Series::new([0u64, 1, 0]));
    let scattered = DataBlock::scatter_block(&raw, &indices, 2)?;
    assert_eq!(scattered.len(), 2);
    assert_eq!(raw.schema(), scattered[0].schema());
    assert_eq!(raw.schema(), scattered[1].schema());
    assert_eq!(scattered[0].num_rows(), 2);
    assert_eq!(scattered[1].num_rows(), 1);

    let expected = vec![
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 1 |",
        "| 3 | 3 |",
        "| 2 | 2 |",
        "+---+---+",
    ];
    crate::assert_blocks_eq(expected, &scattered);

    Ok(())
}
