use std::collections::HashMap;
use serde_derive::{Deserialize, Serialize};

/// Array Data is wrapper around vector containing null map as separate field
#[derive(Clone)]
pub struct ArrayData<T> {
    data: Vec<T>,
    null_bitmap: Option<Vec<bool>>,
}

/// A generic representation of array to be used as data holder in column
#[derive(Clone, Debug)]
pub enum Array {
    I8(ArrayData<i8>),
    U8(ArrayData<u8>),
    I16(ArrayData<i16>),
    U16(ArrayData<u16>),
    I32(ArrayData<i32>),
    U32(ArrayData<u32>),
    I64(ArrayData<i64>),
    U64(ArrayData<u64>),
    I128(ArrayData<i128>),
    U128(ArrayData<u128>),
    F32(ArrayData<f32>),
    F64(ArrayData<f64>),
    BOOL(ArrayData<bool>),
    STRING(ArrayData<String>),
}

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

/// column is a generic data structure for dataframe API and defines common operations
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Column {
    data: Array,
    field: Field
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Schema {
    fields: Vec<Field>,
}