//! Shared helpers for in-module tests (`#[cfg(test)]`).

/// Retry `f` (checked every 10ms, up to 2s) until it returns `Some`, or panic.
pub(crate) async fn wait_for<T>(mut f: impl FnMut() -> Option<T>) -> T {
    for _ in 0..200 {
        if let Some(v) = f() {
            return v;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("condition never became true");
}

/// Bool-predicate convenience wrapper over [`wait_for`].
pub(crate) async fn wait_until(mut f: impl FnMut() -> bool) {
    wait_for(|| f().then_some(())).await;
}
