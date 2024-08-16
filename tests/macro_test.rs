#![allow(dead_code)]

use anyhow::Result as AnyhowResult;
use pyspark_arrow_rs::HasArrowSparkSchema;
use serde::Deserialize;

#[derive(HasArrowSparkSchema, Deserialize)]
struct Test {
    string_field: String,
    smallint_field: i32,
    bigint_field: i64,
    smallfloat_field: f32,
    bigfloat_field: f64,
    bool_field: bool,
    string_vec: Vec<String>,
    int_vec: Vec<i32>,
}

#[test]
fn test_spark_types() -> AnyhowResult<()> {
    let expected = "`string_field` STRING, `smallint_field` INT, `bigint_field` BIGINT, `smallfloat_field` FLOAT, `bigfloat_field` DOUBLE, `bool_field` BOOLEAN, `string_vec` ARRAY<STRING>, `int_vec` ARRAY<INT>".to_string();
    let ddl = Test::get_spark_ddl()?;
    println!("{}", ddl);
    let trunc = &ddl[..expected.len()];
    assert_eq!(expected, trunc);
    Ok(())
}
