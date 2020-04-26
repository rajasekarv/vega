use std::time::Instant;

pub(super) trait ListenerEvent: Send + Sync {
    ///Whether output this event to the event log.
    fn log_event(&self) -> bool {
        true
    }
}

#[derive(Clone, Copy)]
pub(super) struct StageInfo {}

pub(super) struct JobStartListener {
    pub job_id: usize,
    pub time: Instant,
    pub stage_infos: Vec<StageInfo>,
}
impl ListenerEvent for JobStartListener {}

pub(super) struct JobEndListener {
    pub job_id: usize,
    pub time: Instant,
    pub job_result: bool,
}
impl ListenerEvent for JobEndListener {}
