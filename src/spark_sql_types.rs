use std::fmt;

pub(crate) enum SparkSqlType {
    Boolean,
    String,
    Float,
    Double,
    Int,
    BigInt,
    Array,
    Map,
    Struct
}

impl fmt::Display for SparkSqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SparkSqlType::Boolean => write!(f, "BOOLEAN"),
            SparkSqlType::String => write!(f, "STRING"),
            SparkSqlType::Float => write!(f, "FLOAT"),
            SparkSqlType::Double => write!(f, "DOUBLE"),
            SparkSqlType::Int => write!(f, "INT"),
            SparkSqlType::BigInt => write!(f, "BIGINT"),
            SparkSqlType::Array => write!(f, "ARRAY"),
            SparkSqlType::Map => write!(f, "MAP"),
            SparkSqlType::Struct => write!(f, "STRUCT"),
        }
    }
}
