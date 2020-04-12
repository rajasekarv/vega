use downcast_rs::{impl_downcast, DowncastSync};
use serde_traitobject::{Deserialize, Serialize};

pub(crate) struct SplitStruct {
    index: usize,
}

pub trait Split: DowncastSync + dyn_clone::DynClone + Serialize + Deserialize {
    fn get_index(&self) -> usize;
}

impl_downcast!(Split);
dyn_clone::clone_trait_object!(Split);
