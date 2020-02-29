use serde_derive::{Deserialize, Serialize};

/// Array Data is wrapper around vector containing null map as separate field
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrayData<T> {
    data: Vec<T>,
    null_bitmap: Option<Vec<bool>>,
}

/// A generic representation of array to be used as data holder in column
#[derive(Serialize, Deserialize, Debug, Clone)]
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
