//! The crate version (from the workspace's `[workspace.package] version`), shown in
//! the startup banner and the optional web dashboard so a running instance can be
//! matched back to a release.

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
