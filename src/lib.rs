mod spark_sql_types;

use anyhow::{anyhow, Context, Result as AnyhowResult};
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, FieldRef},
};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use spark_sql_types::SparkSqlType;
use std::sync::Arc;

pub trait HasArrowSparkSchema {
    fn get_arrow_schema() -> Vec<Arc<Field>>;

    fn get_spark_ddl() -> AnyhowResult<String>;
}

/// Convert arrow field to spark SQL representation
pub fn field_to_spark(arrow_field: &Arc<Field>) -> AnyhowResult<String> {
    match arrow_field.data_type() {
        DataType::Null => Err(anyhow!("Null not supported")),
        DataType::Boolean => Ok(SparkSqlType::Boolean.to_string()),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => Ok(SparkSqlType::Int.to_string()),
        DataType::Int64 | DataType::UInt64 => Ok(SparkSqlType::BigInt.to_string()),
        DataType::Float16 | DataType::Float32 => Ok(SparkSqlType::Float.to_string()),
        DataType::Float64 => Ok(SparkSqlType::Double.to_string()),
        DataType::Timestamp(_, _) => todo!("Timestamp not yet implemented"),
        DataType::Date32 => todo!("Date32 not yet implemented"),
        DataType::Date64 => todo!("Date64 not yet implemented"),
        DataType::Time32(_) => todo!("Time32 not yet implemented"),
        DataType::Time64(_) => todo!("Time64 not yet implemented"),
        DataType::Duration(_) => todo!("Duration not yet implemented"),
        DataType::Interval(_) => todo!("Interval not yet implemented"),
        DataType::Binary => todo!("Binary not yet implemented"),
        DataType::FixedSizeBinary(_) => todo!("FixedSizeBinary not yet implemented"),
        DataType::LargeBinary => todo!("LargeBinary not yet implemented"),
        DataType::BinaryView => todo!("BinaryView not yet implemented"),
        DataType::Utf8 => Ok(SparkSqlType::String.to_string()),
        DataType::LargeUtf8 => Err(anyhow!("LargeUtf8 not supported. Should have already been converted to utf8. Something went wrong.")),
        DataType::Utf8View => todo!("Utf8View not yet implemented"),
        DataType::List(z) => {
            let inner_type = field_to_spark(z)?;
            let outer_type = SparkSqlType::Array.to_string();
            let array = format!("{}<{}>", outer_type, inner_type);
            Ok(array)
        }
        DataType::ListView(_) => todo!("ListView not yet implemented"),
        DataType::FixedSizeList(_, _) => todo!("FixedSizeList not yet implemented"),
        DataType::LargeList(_) => todo!("LargeList not yet implemented"),
        DataType::LargeListView(_) => todo!("LargeListView not yet implemented"),
        DataType::Struct(fields) => {
            let field_strings: AnyhowResult<Vec<String>> = fields
                .iter()
                .map(|f| {
                    let compatible_field = convert_field_to_compatible(f.clone());
                    let spark_type = field_to_spark(&compatible_field)?;
                    Ok(format!("{}: {}", f.name(), spark_type))
                })
                .collect();
            let struct_fields = field_strings?.join(", ");
            let outer_type = SparkSqlType::Struct.to_string();
            Ok(format!("{}<{}>", outer_type, struct_fields))
        }
        DataType::Union(_, _) => todo!("Union not yet implemented"),
        DataType::Dictionary(_, _) => todo!("Dictionary not yet implemented"),
        DataType::Decimal128(_, _) => todo!("Decimal128 not yet implemented"),
        DataType::Decimal256(_, _) => todo!("Decimal256 not yet implemented"),
        DataType::Map(inner, _) => {
            match inner.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    let key_field = convert_field_to_compatible(fields[0].clone());
                    let value_field = convert_field_to_compatible(fields[1].clone());
                    let key_type = field_to_spark(&key_field)?;
                    let value_type = field_to_spark(&value_field)?;
                    let outer_type = SparkSqlType::Map.to_string();
                    let array = format!("{}<{},{}>", outer_type, key_type, value_type);
                    Ok(array)
                }
                other => Err(anyhow!("Expected Struct with 2 fields for Map, got {:?}", other)),
            }
        }
        DataType::RunEndEncoded(_, _) => todo!("RunEndEncoded not yet implemented"),
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
    let fields = Vec::<FieldRef>::from_type::<A>(TracingOptions::default().map_as_struct(false)).unwrap();
    fields
        .clone()
        .into_iter()
        .map(convert_field_to_compatible)
        .collect()
}

fn convert_field_to_compatible(field: Arc<Field>) -> Arc<Field> {
    match field.data_type() {
        // Large list is not supported so we convert to list
        DataType::LargeList(inner) => {
            let converted_inner = convert_field_to_compatible(inner.clone());
            Arc::new(
                (*field)
                    .clone()
                    .with_data_type(DataType::List(converted_inner)),
            )
        }
        // Largeutf8 is not supported so we convert it to utf8
        DataType::LargeUtf8 => Arc::new((*field).clone().with_data_type(DataType::Utf8)),
        _ => field,
    }
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
