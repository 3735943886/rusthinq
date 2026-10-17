//! Engine-independent resource budgets shared by configuration and driver preparation.
use serde::Deserialize;
use std::io;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub source_bytes: usize,
    pub string_bytes: usize,
    pub operations: u64,
    pub outputs: usize,
    pub output_bytes: usize,
    pub state_keys: usize,
    pub state_bytes: usize,
    pub worker_capacity: usize,
    pub input_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            source_bytes: 524288,
            string_bytes: 131072,
            operations: 5_000_000,
            outputs: 256,
            output_bytes: 1_048_576,
            state_keys: 256,
            state_bytes: 262144,
            worker_capacity: 16,
            input_bytes: 131072,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> io::Result<()> {
        let sizes = [
            self.source_bytes,
            self.string_bytes,
            self.outputs,
            self.output_bytes,
            self.state_keys,
            self.state_bytes,
            self.worker_capacity,
            self.input_bytes,
        ];
        // Rhai uses zero as unlimited. Reject it, allocation overflow and queue
        // capacities exceeding Tokio's portable semaphore bound.
        if self.operations == 0
            || sizes.iter().any(|n| *n == 0 || *n > isize::MAX as usize)
            || self.worker_capacity > usize::MAX >> 3
            || self
                .input_bytes
                .checked_add(self.output_bytes)
                .and_then(|bytes| bytes.checked_mul(self.worker_capacity))
                .filter(|bytes| *bytes <= isize::MAX as usize)
                .is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid driver limits",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounds_reject_unlimited_and_overflowing_budgets() {
        Limits::default().validate().unwrap();
        for limits in [
            Limits {
                operations: 0,
                ..Limits::default()
            },
            Limits {
                source_bytes: 0,
                ..Limits::default()
            },
            Limits {
                output_bytes: usize::MAX,
                ..Limits::default()
            },
            Limits {
                worker_capacity: usize::MAX,
                ..Limits::default()
            },
            Limits {
                worker_capacity: 16,
                input_bytes: isize::MAX as usize,
                ..Limits::default()
            },
            Limits {
                outputs: usize::MAX,
                string_bytes: 3,
                ..Limits::default()
            },
        ] {
            assert!(limits.validate().is_err(), "{limits:?}");
        }
    }
}
