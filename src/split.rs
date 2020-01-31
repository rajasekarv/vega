// not necessary I guess
use downcast_rs::DowncastSync;
use dyn_clone;
use serde_traitobject::{Deserialize, Serialize};
use std::net::Ipv4Addr;

pub struct SplitStruct {
    index: usize,
}

pub trait Split: DowncastSync + dyn_clone::DynClone + Serialize + Deserialize {
    fn get_index(&self) -> usize;
}

impl_downcast!(Split);
dyn_clone::clone_trait_object!(Split);
