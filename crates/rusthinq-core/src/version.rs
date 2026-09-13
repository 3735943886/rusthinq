//! Build-time git revision (see `build.rs`), shown in the startup banner and the
//! optional web dashboard so a running instance can be matched back to a commit.

pub const REVISION: &str = env!("RUSTHINQ_REVISION");
