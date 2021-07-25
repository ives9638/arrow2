
use common_exception::Result;

use crate::*;
use arrow2::api::prelude::DataType;

#[test]
fn test_data_block_sort() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Utf8, false),
    ]);

    let raw = DataBlock::create_by_array(schema.clone(), vec![
        Series::new(vec![6, 4, 3, 2, 1, 7]),
        Series::new(vec!["b1", "b2", "b3", "b4", "b5", "b6"]),
    ]);

    {
        let options = vec![SortColumnDescription {
            column_name: "a".to_owned(),
            asc: true,
            nulls_first: false,
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 1 | b5 |",
            "| 2 | b4 |",
            "| 3 | b3 |",
            "+---+----+",
        ];
        crate::assert_blocks_eq(expected, &[results]);
    }

    {
        let options = vec![SortColumnDescription {
            column_name: "a".to_owned(),
            asc: false,
            nulls_first: false,
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 7 | b6 |",
            "| 6 | b1 |",
            "| 4 | b2 |",
            "+---+----+",
        ];
        crate::assert_blocks_eq(expected, &[results]);
    }
    Ok(())
}
