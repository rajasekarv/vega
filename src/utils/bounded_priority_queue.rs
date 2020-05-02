use crate::serializable_traits::Data;
use serde_derive::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct BoundedMinPriorityQueue<T: Ord> {
    max_size: usize,
    underlying: BinaryHeap<Reverse<T>>,
}

impl<T: Data + Ord> BoundedMinPriorityQueue<T> {
    pub fn new(max_size: usize) -> BoundedMinPriorityQueue<T> {
        BoundedMinPriorityQueue {
            max_size: max_size,
            underlying: BinaryHeap::with_capacity(max_size),
        }
    }

    pub fn get_size(&self) -> usize {
        self.underlying.len()
    }

    pub fn into_vec_sorted(&self) -> Vec<T> {
        let mut res = self
            .underlying
            .clone()
            .into_iter_sorted()
            .map(|Reverse(i)| i)
            .collect::<Vec<_>>();
        res.reverse();
        res
    }

    pub fn merge(&mut self, other: BoundedMinPriorityQueue<T>) -> &Self {
        other
            .underlying
            .into_iter()
            .for_each(|Reverse(elem)| self.append(elem));
        self
    }

    pub fn append(&mut self, elem: T) {
        if self.underlying.len() < self.max_size {
            self.underlying.push(Reverse(elem));
        } else {
            self.maybe_replace_lowest(elem);
        }
    }

    pub(self) fn maybe_replace_lowest(&mut self, elem: T)
    where
        T: Data + Ord,
    {
        if let Some(Reverse(head)) = self.underlying.peek() {
            if elem.gt(head) {
                self.underlying.pop();
                self.underlying.push(Reverse(elem));
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct BoundedMaxPriorityQueue<T: Ord> {
    max_size: usize,
    underlying: BinaryHeap<T>,
}

impl<T: Data + Ord> BoundedMaxPriorityQueue<T> {
    pub fn new(max_size: usize) -> BoundedMaxPriorityQueue<T> {
        BoundedMaxPriorityQueue {
            max_size: max_size,
            underlying: BinaryHeap::with_capacity(max_size),
        }
    }

    pub fn get_size(&self) -> usize {
        self.underlying.len()
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

    pub fn merge(&mut self, other: BoundedMaxPriorityQueue<T>) -> &Self {
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
