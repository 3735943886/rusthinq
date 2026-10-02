//! L5 driver preparation admission and bounded input buffering. No lifecycle ownership.
use std::collections::{BTreeMap, VecDeque};
pub struct Preparation<K> {
    capacity: usize,
    attempts: BTreeMap<String, (K, String)>,
    buffers: BTreeMap<String, (K, VecDeque<Vec<u8>>)>,
}
impl<K: Clone + PartialEq> Preparation<K> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            attempts: BTreeMap::new(),
            buffers: BTreeMap::new(),
        }
    }
    pub fn can_start(in_flight: usize, worker_exists: bool) -> bool {
        in_flight < 2 && !worker_exists
    }
    pub fn begin(&mut self, id: &str, scope: K, model: &str) -> bool {
        if self.is_current(id, &scope, model)
            || (!self.attempts.contains_key(id) && self.attempts.len() >= self.capacity)
        {
            return false;
        }
        self.attempts.insert(id.into(), (scope, model.into()));
        true
    }
    pub fn is_current(&self, id: &str, scope: &K, model: &str) -> bool {
        self.attempts
            .get(id)
            .is_some_and(|(current, name)| current == scope && name == model)
    }
    pub fn buffer(&mut self, id: &str, scope: K, data: &[u8]) -> Result<(), String> {
        let total: usize = self
            .buffers
            .values()
            .flat_map(|(_, queue)| queue.iter())
            .map(Vec::len)
            .sum();
        let stale: usize = self
            .buffers
            .get(id)
            .filter(|(current, _)| current != &scope)
            .map(|(_, queue)| queue.iter().map(Vec::len).sum())
            .unwrap_or(0);
        if data.len() > 65536
            || total.saturating_sub(stale).saturating_add(data.len()) > 1_048_576
            || (!self.buffers.contains_key(id) && self.buffers.len() >= self.capacity)
        {
            return Err("driver input preparation budget exceeded".into());
        }
        let buffered = self
            .buffers
            .entry(id.into())
            .or_insert_with(|| (scope.clone(), VecDeque::new()));
        if buffered.0 != scope {
            *buffered = (scope, VecDeque::new());
        }
        if buffered.1.len() >= 16 {
            return Err("driver input preparation queue exceeded".into());
        }
        buffered.1.push_back(data.to_vec());
        Ok(())
    }
    pub fn take_buffer(&mut self, id: &str, scope: &K) -> VecDeque<Vec<u8>> {
        if self
            .buffers
            .get(id)
            .is_some_and(|(current, _)| current == scope)
        {
            self.buffers.remove(id).expect("current buffer").1
        } else {
            VecDeque::new()
        }
    }
    pub fn retain(&mut self, mut current: impl FnMut(&str, &K) -> bool) {
        self.attempts.retain(|id, (scope, _)| current(id, scope));
        self.buffers.retain(|id, (scope, _)| current(id, scope));
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failed_attempt_is_not_retried_and_stale_completion_cannot_take_successor_input() {
        let mut policy = Preparation::new(2);
        assert!(policy.begin("a", 1, "model"));
        assert!(!policy.begin("a", 1, "model"));
        policy.buffer("a", 1, b"old").unwrap();
        assert!(policy.begin("a", 2, "model"));
        policy.buffer("a", 2, b"new").unwrap();
        assert!(policy.take_buffer("a", &1).is_empty());
        assert_eq!(
            policy.take_buffer("a", &2),
            VecDeque::from([b"new".to_vec()])
        );
        policy.retain(|_, scope| *scope == 2);
        assert!(!policy.is_current("a", &1, "model"));
        assert!(Preparation::<u64>::can_start(1, false));
        assert!(!Preparation::<u64>::can_start(2, false));
        assert!(!Preparation::<u64>::can_start(0, true));
    }
    #[test]
    fn global_and_per_device_budget_and_replacement_release_are_bounded() {
        let mut policy = Preparation::new(2);
        let frame = vec![0; 65536];
        for _ in 0..16 {
            policy.buffer("a", 1, &frame).unwrap();
        }
        assert!(policy.buffer("a", 1, b"overflow").is_err());
        assert!(policy.buffer("b", 1, b"overflow").is_err());
        policy.buffer("a", 2, b"replacement").unwrap();
        policy.buffer("b", 1, b"independent").unwrap();
        assert!(policy.buffer("third", 1, b"capacity").is_err());
        assert!(policy.buffer("a", 2, &vec![0; 65537]).is_err());
        policy.retain(|id, _| id == "b");
        assert!(policy.take_buffer("a", &2).is_empty());
        assert_eq!(
            policy.take_buffer("b", &1),
            VecDeque::from([b"independent".to_vec()])
        );
    }
}
