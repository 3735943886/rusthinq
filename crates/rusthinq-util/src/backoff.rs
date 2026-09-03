//! Exponential backoff with jitter for a "reconnect forever" loop — shared by every
//! place that keeps retrying a persistent upstream connection (the MQTT control-plane
//! client, the LG-cloud ThinQ1/ThinQ2 bridge sessions): a fixed retry interval either
//! hammers a real outage at full rate for as long as it lasts, or is slow to recover
//! from a one-off blip if set conservatively. Backing off (and resetting once a
//! connection actually succeeds) gets both: fast recovery from a blip, gentler
//! long-run retry rate against a real outage.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct ExponentialBackoff {
    initial: Duration,
    max: Duration,
    current: Duration,
}

impl ExponentialBackoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            current: initial,
        }
    }

    /// For a connection to something on the same machine/LAN as this process (the
    /// local MQTT control-plane broker) — a real outage there is very likely something
    /// the same operator controls, so it's fine to retry fast and still cap low.
    pub fn for_local_control_plane() -> Self {
        Self::new(Duration::from_millis(500), Duration::from_secs(30))
    }

    /// For a connection to a real third-party vendor's cloud (LG's ThinQ1/ThinQ2
    /// upstream, one per bridged device) — retried at a fixed rate by every bridged
    /// device at once during a real outage there is a "hammer a third party" risk, so
    /// this starts slower and caps higher than the local-control-plane profile.
    pub fn for_external_upstream() -> Self {
        Self::new(Duration::from_secs(2), Duration::from_secs(60))
    }

    /// The delay to wait before the next attempt; doubles (capped at `max`) for next
    /// time, so consecutive failures back off. +/-20% jitter so several callers
    /// hitting the same outage at once (e.g. multiple bridged devices losing the same
    /// upstream together) don't all retry in lockstep.
    pub fn next_delay(&mut self) -> Duration {
        let base = self.current;
        self.current = (self.current * 2).min(self.max);
        jitter(base)
    }

    /// Call once a connection attempt actually succeeds, so the *next* failure streak
    /// starts backing off from `initial` again rather than wherever this one left off.
    pub fn reset(&mut self) {
        self.current = self.initial;
    }
}

fn jitter(base: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    // 800..1200 (i.e. 0.8x..1.2x) from the low bits of the current time - not
    // cryptographic, just enough spread to avoid a lockstep retry storm.
    let per_mille = 800 + u64::from(nanos % 400);
    Duration::from_nanos((base.as_nanos() as u64).saturating_mul(per_mille) / 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_delay_is_close_to_initial() {
        let mut b = ExponentialBackoff::new(Duration::from_millis(500), Duration::from_secs(30));
        let d = b.next_delay();
        assert!(d >= Duration::from_millis(400) && d <= Duration::from_millis(600));
    }

    #[test]
    fn consecutive_failures_double_up_to_the_cap() {
        let mut b = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(8));
        let delays: Vec<Duration> = (0..6).map(|_| b.next_delay()).collect();
        // jittered +/-20%, so compare against the un-jittered doubling sequence with slack
        let expected_bases = [1u64, 2, 4, 8, 8, 8];
        for (d, base) in delays.iter().zip(expected_bases) {
            let lo = Duration::from_millis(base * 800);
            let hi = Duration::from_millis(base * 1200);
            assert!(
                *d >= lo && *d <= hi,
                "{d:?} not in [{lo:?}, {hi:?}] for base {base}s"
            );
        }
    }

    #[test]
    fn reset_starts_the_next_failure_streak_from_initial_again() {
        let mut b = ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(10));
        b.next_delay();
        b.next_delay();
        b.next_delay(); // now backed off well past initial
        b.reset();
        let d = b.next_delay();
        assert!(d >= Duration::from_millis(80) && d <= Duration::from_millis(120));
    }
}
