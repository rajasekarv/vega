use crate::dependency::ShuffleDependencyTrait;
use crate::rdd::RddBase;
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::Arc;

// this is strange. see into this in more detail
#[derive(Clone)]
pub(crate) struct Stage {
    pub id: usize,
    pub num_partitions: usize,
    pub shuffle_dependency: Option<Arc<dyn ShuffleDependencyTrait>>,
    pub is_shuffle_map: bool,
    pub rdd: Arc<dyn RddBase>,
    pub parents: Vec<Stage>,
    pub output_locs: Vec<Vec<String>>,
    pub num_available_outputs: usize,
}

impl PartialOrd for Stage {
    fn partial_cmp(&self, other: &Stage) -> Option<Ordering> {
        Some(self.id.cmp(&other.id))
    }
}

impl PartialEq for Stage {
    fn eq(&self, other: &Stage) -> bool {
        self.id == other.id
    }
}

impl Eq for Stage {}

impl Ord for Stage {
    fn cmp(&self, other: &Stage) -> Ordering {
        self.id.cmp(&other.id)
    }
}
impl Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Stage {}", self.id)
    }
}
impl Stage {
    pub fn get_rdd(&self) -> Arc<dyn RddBase> {
        self.rdd.clone()
    }

    pub fn new(
        id: usize,
        rdd: Arc<dyn RddBase>,
        shuffle_dependency: Option<Arc<dyn ShuffleDependencyTrait>>,
        parents: Vec<Stage>,
    ) -> Self {
        Stage {
            id,
            num_partitions: rdd.number_of_splits(),
            is_shuffle_map: shuffle_dependency.clone().is_some(),
            shuffle_dependency,
            parents,
            rdd: rdd.clone(),
            output_locs: {
                let mut v = Vec::new();
                for _ in 0..rdd.number_of_splits() {
                    v.push(Vec::new());
                }
                v
            },
            num_available_outputs: 0,
        }
    }

    pub fn is_available(&self) -> bool {
        if self.parents.is_empty() && !self.is_shuffle_map {
            true
        } else {
            log::debug!(
                "num available outputs {}, and num partitions {}, in is available method in stage",
                self.num_available_outputs,
                self.num_partitions
            );
            self.num_available_outputs == self.num_partitions
        }
    }

    pub fn add_output_loc(&mut self, partition: usize, host: String) {
        log::debug!(
            "adding loc for partition inside stage {} @{}",
            partition,
            host
        );
        if !self.output_locs[partition].is_empty() {
            self.num_available_outputs += 1;
        }
        self.output_locs[partition].push(host);
    }

    pub fn remove_output_loc(&mut self, partition: usize, host: &str) {
        let prev_vec = self.output_locs[partition].clone();
        let new_vec = prev_vec
            .clone()
            .into_iter()
            .filter(|x| x != host)
            .collect::<Vec<_>>();
        if (!prev_vec.is_empty()) && (new_vec.is_empty()) {
            self.num_available_outputs -= 1;
        }
        self.output_locs[partition] = new_vec;
    }
}
