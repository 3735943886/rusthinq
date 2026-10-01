//! Embed short git SHA for the management UI (or "dev" when unavailable).
//!
//! Order:
//! 1. `RUSTHINQ_GIT_SHA` env (Docker build-arg / CI)
//! 2. `git rev-parse --short=7 HEAD`
//! 3. literal `dev`

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=RUSTHINQ_GIT_SHA");
    // Best-effort: rebuild when HEAD moves (path missing in Docker is fine).
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs/heads");

    let sha = std::env::var("RUSTHINQ_GIT_SHA")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && s != "unknown")
        .or_else(git_short_sha)
        .unwrap_or_else(|| "dev".to_string());

    // Keep nav badge short even if a full SHA is passed.
    let short: String = sha.chars().take(7).collect();
    println!("cargo:rustc-env=RUSTHINQ_GIT_SHA={short}");
}

fn git_short_sha() -> Option<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--short=7", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    let s = s.trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}
