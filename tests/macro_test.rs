#![allow(dead_code)]

use std::collections::HashMap;
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
#[derive(HasArrowSparkSchema, Deserialize)]
struct MapTest {
    string_field: String,
    map_field: HashMap<String, Test>,
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

#[test]
fn test_map_types() -> AnyhowResult<()> {
    let expected = "`string_field` STRING, `map_field` MAP<STRING,STRUCT<string_field: STRING, smallint_field: INT, bigint_field: BIGINT, smallfloat_field: FLOAT, bigfloat_field: DOUBLE, bool_field: BOOLEAN, string_vec: ARRAY<STRING>, int_vec: ARRAY<INT>>>".to_string();
    let ddl = MapTest::get_spark_ddl()?;
    println!("{}", ddl);
    let trunc = &ddl[..expected.len()];
    assert_eq!(expected, trunc);
    Ok(())
}