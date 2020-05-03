use crate::serializable_traits::Data;
use serde_derive::{Deserialize, Serialize};
use std::collections::BinaryHeap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct BoundedPriorityQueue<T: Ord> {
    max_size: usize,
    underlying: BinaryHeap<T>,
}

impl<T: Data + Ord> BoundedPriorityQueue<T> {
    pub fn new(max_size: usize) -> BoundedPriorityQueue<T> {
        BoundedPriorityQueue {
            max_size: max_size,
            underlying: BinaryHeap::with_capacity(max_size),
        }
    }

    pub fn into_vec_sorted(&self) -> Vec<T> {
        let mut res = self
            .underlying
            .clone()
            .into_iter_sorted()
            .collect::<Vec<_>>();
        res.reverse();
        res
    }

    pub fn merge(&mut self, other: BoundedPriorityQueue<T>) -> &Self {
        other
            .underlying
            .into_iter()
            .for_each(|elem| self.append(elem));
        self
    }

    pub fn append(&mut self, elem: T) {
        if self.underlying.len() < self.max_size {
            self.underlying.push(elem);
        } else {
            self.maybe_replace_lowest(elem);
        }
    }

    pub(self) fn maybe_replace_lowest(&mut self, elem: T)
    where
        T: Data + Ord,
    {
        if let Some(head) = self.underlying.peek() {
            if elem.lt(head) {
                self.underlying.pop();
                self.underlying.push(elem);
            }
        }
    }
}
