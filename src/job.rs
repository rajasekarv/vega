use std::cmp::Ordering;

#[derive(Clone, Debug)]
pub struct Job {
    run_id: usize,
    job_id: usize,
}

impl Job {
    pub fn new(run_id: usize, job_id: usize) -> Self {
        Job { run_id, job_id }
    }
}

// manual ordering implemented because we want the jobs to sorted in reverse order
impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Job) -> Option<Ordering> {
        Some(other.job_id.cmp(&self.job_id))
    }
}

impl PartialEq for Job {
    fn eq(&self, other: &Job) -> bool {
        self.job_id == other.job_id
    }
}

impl Eq for Job {}

impl Ord for Job {
    fn cmp(&self, other: &Job) -> Ordering {
        other.job_id.cmp(&self.job_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sort_job() {
        let mut jobs = vec![Job::new(1, 2), Job::new(1, 1), Job::new(1, 3)];
        println!("{:?}", jobs);
        jobs.sort();
        println!("{:?}", jobs);
        assert_eq!(jobs, vec![Job::new(1, 3), Job::new(1, 2), Job::new(1, 1),])
    }
}
