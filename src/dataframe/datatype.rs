use serde_derive::{Deserialize, Serialize};

/// DataType represents the datatypes natively supported in dataframe API. Naming conventions are
/// retained from spark sql types for equivalent types. In addition to spark sql types, more types are supported here
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DataType {
    BooleanType,
    ShortType,
    IntegerType,
    LongType,
    UnsignedShortType,
    UnsignedIntegerType,
    UnsignedLongType,
    U128Type,
    I128Type,
    ByteType,
    FloatType,
    DoubleType,
    BigDecimal,
    StringType,
    BinaryType,
    TimeStampType,
    DateType,
    ArrayType{data_type: Box<DataType>, contains_null: bool},
    MapType{key_type: Box<DataType>, value_type: Box<DataType>, value_contains_null: bool},
    StructType(Vec<Field>)
}

/// meta data of column type
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Field{
    name: String,
    data_type: DataType,
    nullable: bool
}
