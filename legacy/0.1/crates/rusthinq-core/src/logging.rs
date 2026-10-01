//! Simple topic-filtered logging (mirrors util/logging.ts).

use rusthinq_util::sync::RwLock;
use std::sync::OnceLock;

type LogFilter = Box<dyn Fn(&str) -> bool + Send + Sync>;

static FILTER: OnceLock<RwLock<LogFilter>> = OnceLock::new();

fn filter_slot() -> &'static RwLock<LogFilter> {
    FILTER.get_or_init(|| RwLock::new(Box::new(|_: &str| true)))
}

pub fn set_filter<F>(f: F)
where
    F: Fn(&str) -> bool + Send + Sync + 'static,
{
    *filter_slot().write() = Box::new(f);
}

pub fn log(topic: &str, args: &[&str]) {
    if filter_slot().read()(topic) {
        tracing::info!(topic, "{}", args.join(" "));
    }
}

/// Silence all logging (used by device unit tests).
pub fn silence() {
    set_filter(|_| false);
}
