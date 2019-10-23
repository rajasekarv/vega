use super::*;
// not necessary I guess
use downcast_rs::Downcast;
use objekt;
pub struct SplitStruct {
    index: usize,
}

pub trait Split: Downcast + objekt::Clone + Serialize + Deserialize {
    fn get_index(&self) -> usize;
}
impl_downcast!(Split);
objekt::clone_trait_object!(Split);
