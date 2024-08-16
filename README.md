# Pyspark Arrow Rust (pyspark-arrow-rs)
Intention is to be used together with Pyo3 (maturin), arrow-rs, serde_arrow to generate python code that can be used in Spark for ETL jobs

## Context
The naive way to try to use Spark would be to use Spark UDFs or Pandas UDFs. Spark is natively written in the JVM, and interops with Python through Py4J. The conversion overhead from a Java RDD into something that a Python UDF can process is quite high.

As such, the way around this is to use Apache Arrow. PySpark already uses Arrow for interop between JVM and Python, and by directly leveraging the Arrow format, we are able to skip any additional overhead and directly read the Arrow object in a Rust function.

```
RDD -> Spark (Java) -> Arrow -> Pyspark -> Rust
```

## How to do this
We are able to directly load Arrow objects into a Rust function wrapped in Python (through Maturin) through the `pyspark.sql.Dataframe.mapInArrow` API.

This API expects two arguments:
* A Python function that yields an Apache Arrow RecordBatch item
* The expected schema

We are able to write a Rust function that returns an arrow-rs RecordBatch item that can be yielded by the python function.

We are also able to pass in the expected schema by using serde_arrow (and a macro provided by this crate) to derive the schema.

## TL;DR
This crate provides some macros that make life easier to be used together with `mapInArrow` as well as provide some recipes on how to leverage Rust in Pyspark

### Example Usage
* Refer to `tests` folder for a real example. But the basic idea is, given a struct, you can generate a spark DDL to be used in `map_in_arrow` on a pyspark dataframe
```rust
use pyspark_arrow_rs::HasArrowSparkSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, HasArrowSparkSchema)]
pub(crate) struct TestStruct {
    pub(crate) col: String,
    pub(crate) int_col: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_macro() {
        let _a = TestStruct {
            col: "bla".to_string(),
            int_col: 1000,
        };
        let ddl = TestStruct::get_spark_ddl();
        assert_eq!("`col` STRING, `int_col` BIGINT", ddl.unwrap());
    }
}
```
