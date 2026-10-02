//! L5 invocation ordering, admission and timer decisions. The caller executes effects.
use crate::Error;
use std::collections::{BTreeMap, VecDeque};
use tokio::time::Instant;

pub struct Scheduler<C, T> {
    capacity: usize,
    sequence: u64,
    pending: BTreeMap<u64, String>,
    order: BTreeMap<String, VecDeque<u64>>,
    completed: BTreeMap<u64, T>,
    timers: BTreeMap<(String, String), (C, Instant)>,
}
impl<C, T> Scheduler<C, T> {
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            capacity,
            sequence: 0,
            pending: BTreeMap::new(),
            order: BTreeMap::new(),
            completed: BTreeMap::new(),
            timers: BTreeMap::new(),
        })
    }
    /// Buffered completions still consume admission budget until their ordered release.
    pub fn next_sequence(&self) -> Result<u64, Error> {
        if self.pending.len() >= self.capacity {
            return Err(Error::Busy);
        }
        self.sequence
            .checked_add(1)
            .ok_or(Error::GenerationExhausted)
    }
    /// Commit only after worker admission succeeds, without yielding between these steps.
    pub fn admitted(&mut self, device: String, sequence: u64) -> Result<(), Error> {
        if self.next_sequence()? != sequence || device.is_empty() {
            return Err(Error::InvalidConfig);
        }
        self.sequence = sequence;
        self.pending.insert(sequence, device.clone());
        self.order.entry(device).or_default().push_back(sequence);
        Ok(())
    }
    pub fn complete(
        &mut self,
        sequence: u64,
        device: &str,
        value: T,
    ) -> Result<Vec<(u64, String, T)>, Error> {
        if self.pending.get(&sequence).map(String::as_str) != Some(device)
            || self.completed.contains_key(&sequence)
        {
            return Err(Error::Stale);
        }
        self.completed.insert(sequence, value);
        let queue = self.order.get_mut(device).expect("admitted device queue");
        let mut ready = Vec::new();
        while let Some(sequence) = queue.front().copied() {
            let Some(value) = self.completed.remove(&sequence) else {
                break;
            };
            queue.pop_front();
            self.pending.remove(&sequence);
            ready.push((sequence, device.to_owned(), value));
        }
        if queue.is_empty() {
            self.order.remove(device);
        }
        Ok(ready)
    }
    pub fn delivery_available(&self, in_flight: usize) -> bool {
        in_flight < self.capacity
    }
    pub fn set_timer(
        &mut self,
        device: String,
        name: String,
        context: C,
        after_ms: Option<u64>,
        callback_enabled: bool,
        now: Instant,
    ) -> Result<(), String> {
        let key = (device.clone(), name);
        let Some(after_ms) = after_ms else {
            self.timers.remove(&key);
            return Ok(());
        };
        if !callback_enabled {
            return Err("timer callback disabled".into());
        }
        if !self.timers.contains_key(&key)
            && self.timers.keys().filter(|(id, _)| id == &device).count() >= 64
        {
            return Err("timer capacity exceeded".into());
        }
        let deadline = now
            .checked_add(std::time::Duration::from_millis(after_ms))
            .ok_or("timer deadline exceeded")?;
        self.timers.insert(key, (context, deadline));
        Ok(())
    }
    pub fn next_deadline(&self) -> Option<Instant> {
        self.timers.values().map(|(_, deadline)| *deadline).min()
    }
    pub fn due_timers(&mut self, now: Instant) -> Vec<(String, String, C)> {
        let due: Vec<_> = self
            .timers
            .iter()
            .filter(|(_, (_, deadline))| *deadline <= now)
            .map(|(key, _)| key.clone())
            .collect();
        due.into_iter()
            .map(|(device, name)| {
                let (context, _) = self
                    .timers
                    .remove(&(device.clone(), name.clone()))
                    .expect("due timer");
                (device, name, context)
            })
            .collect()
    }
    pub fn retain_timers(&mut self, mut keep: impl FnMut(&str, &C) -> bool) {
        self.timers
            .retain(|(device, _), (context, _)| keep(device, context));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ordered_release_keeps_budget_and_rejects_wrong_duplicate_and_late_completions() {
        let mut policy = Scheduler::<(), &str>::new(3).unwrap();
        for device in ["a", "a", "b"] {
            let sequence = policy.next_sequence().unwrap();
            policy.admitted(device.into(), sequence).unwrap();
        }
        assert!(matches!(policy.next_sequence(), Err(Error::Busy)));
        assert!(policy.complete(2, "b", "foreign").is_err());
        assert!(policy.complete(2, "a", "second").unwrap().is_empty());
        assert!(policy.complete(2, "a", "duplicate").is_err());
        assert!(matches!(policy.next_sequence(), Err(Error::Busy)));
        assert_eq!(
            policy.complete(3, "b", "independent").unwrap(),
            vec![(3, "b".into(), "independent")]
        );
        assert_eq!(
            policy.complete(1, "a", "first").unwrap(),
            vec![(1, "a".into(), "first"), (2, "a".into(), "second")]
        );
        assert!(policy.complete(1, "a", "late").is_err());
        assert_eq!(policy.next_sequence().unwrap(), 4);
    }
    #[test]
    fn timer_replacement_limits_cancellation_and_scope_pruning_are_explicit() {
        let mut policy = Scheduler::<u64, ()>::new(1).unwrap();
        let now = Instant::now();
        for n in 0..64 {
            policy
                .set_timer("a".into(), n.to_string(), 1, Some(10), true, now)
                .unwrap();
        }
        assert!(
            policy
                .set_timer("a".into(), "overflow".into(), 1, Some(10), true, now)
                .is_err()
        );
        policy
            .set_timer("a".into(), "0".into(), 2, Some(20), true, now)
            .unwrap();
        policy
            .set_timer("a".into(), "1".into(), 1, None, false, now)
            .unwrap();
        policy
            .set_timer("b".into(), "independent".into(), 3, Some(5), true, now)
            .unwrap();
        policy.retain_timers(|_, context| *context != 1);
        assert_eq!(
            policy.next_deadline(),
            Some(now + std::time::Duration::from_millis(5))
        );
        assert_eq!(
            policy.due_timers(now + std::time::Duration::from_millis(10)),
            vec![("b".into(), "independent".into(), 3)]
        );
        assert_eq!(
            policy.due_timers(now + std::time::Duration::from_millis(20)),
            vec![("a".into(), "0".into(), 2)]
        );
        assert!(
            policy
                .due_timers(now + std::time::Duration::from_millis(21))
                .is_empty()
        );
        assert!(
            policy
                .set_timer("a".into(), "disabled".into(), 1, Some(1), false, now)
                .is_err()
        );
    }
}
