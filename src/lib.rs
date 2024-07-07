use anyhow::{anyhow, Context, Result as AnyhowResult};
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, FieldRef},
};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use std::sync::Arc;

pub trait HasArrowSparkSchema {
    fn get_arrow_schema() -> Vec<Arc<Field>>;

    fn get_spark_ddl() -> AnyhowResult<String>;
}

/// Convert arrow field to spark SQL representation
pub fn field_to_spark(arrow_field: &Arc<Field>) -> AnyhowResult<String> {
    match arrow_field.data_type() {
        DataType::Null => Err(anyhow!("Null not supported")),
        DataType::Boolean => Ok("BOOLEAN".to_owned()),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => Ok("INT".to_owned()),
        DataType::Int64 | DataType::UInt64 => Ok("BIGINT".to_owned()),
        DataType::Float16 | DataType::Float32 => Ok("FLOAT".to_owned()),
        DataType::Float64 => Ok("DOUBLE".to_owned()),
        DataType::Timestamp(_, _) => todo!(),
        DataType::Date32 => todo!(),
        DataType::Date64 => todo!(),
        DataType::Time32(_) => todo!(),
        DataType::Time64(_) => todo!(),
        DataType::Duration(_) => todo!(),
        DataType::Interval(_) => todo!(),
        DataType::Binary => todo!(),
        DataType::FixedSizeBinary(_) => todo!(),
        DataType::LargeBinary => todo!(),
        DataType::BinaryView => todo!(),
        DataType::Utf8 => Ok("STRING".to_owned()),
        DataType::LargeUtf8 => todo!(),
        DataType::Utf8View => todo!(),
        DataType::List(z) => {
            let inner = field_to_spark(z)?;
            let array = format!("ARRAY<{}>", inner);
            Ok(array)
        }
        DataType::ListView(_) => todo!(),
        DataType::FixedSizeList(_, _) => todo!(),
        DataType::LargeList(_) => todo!(),
        DataType::LargeListView(_) => todo!(),
        DataType::Struct(_) => todo!(),
        DataType::Union(_, _) => todo!(),
        DataType::Dictionary(_, _) => todo!(),
        DataType::Decimal128(_, _) => todo!(),
        DataType::Decimal256(_, _) => todo!(),
        DataType::Map(_, _) => todo!(),
        DataType::RunEndEncoded(_, _) => todo!(),
    }
}

/// Get spark DDL as a string
pub fn get_spark_ddl(fields: Vec<Arc<Field>>) -> AnyhowResult<String> {
    let array: Vec<String> = fields
        .into_iter()
        .try_fold(
            Vec::<String>::new(),
            |mut acc, field| -> AnyhowResult<Vec<String>> {
                let spark_type = field_to_spark(&field).unwrap();
                let field_name = field.name();
                let full_string = format!("`{}` {}", field_name, spark_type);
                acc.push(full_string);
                Ok(acc)
            },
        )
        .context("Failed to get spark type")?;
    Ok(array.join(", "))
}

/// Depending on serde_arrow, get the fields and do some cleanup for unsupported types like largeutf8
pub fn get_arrow_schema<A>() -> Vec<Arc<Field>>
where
    A: for<'a> serde::Deserialize<'a>,
{
    let fields = Vec::<FieldRef>::from_type::<A>(TracingOptions::default()).unwrap();
    fields
        .clone()
        .into_iter()
        .map(|field| match field.data_type() {
            DataType::LargeList(y) => {
                Arc::new((*field).clone().with_data_type(DataType::List(y.clone())))
            }
            DataType::LargeUtf8 => Arc::new((*field).clone().with_data_type(DataType::Utf8)),
            _ => field,
        })
        .collect()
}

/// Generic function to convert vec of structs into a recordbatch
pub fn to_record_batch<A>(input: Vec<A>) -> AnyhowResult<RecordBatch>
where
    A: HasArrowSparkSchema + serde::Serialize + std::fmt::Debug,
{
    let fields = A::get_arrow_schema();
    let record_batch = serde_arrow::to_record_batch(&fields, &input)
        .with_context(|| format!("Failed to convert to Arrow Record Batch: {:?}", input))?;
    Ok(record_batch)
}

// Reexport Macro
pub use pyspark_arrow_rs_impl::HasArrowSparkSchema;
