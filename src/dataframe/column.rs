use crate::dataframe::datatype::Field;
use crate::dataframe::array::Array;
use serde_derive::{Deserialize, Serialize};

/// column is a generic data structure for dataframe API and defines common operations
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    data: Array,
    field: Field,
}
