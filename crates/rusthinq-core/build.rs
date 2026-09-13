//! Bakes a git revision into the build for `version::REVISION` (see `src/version.rs`).
//!
//! `GIT_REVISION` takes precedence when set: a Docker build has no `.git` in its
//! build context, so CI passes the revision in explicitly instead. Otherwise this
//! shells out to `git describe`, which works for a local `cargo build` from a clone.
//! Either way, if nothing yields a revision (`git` missing, not a repo, shallow clone
//! with no tags reachable), the fallback is the literal string "unknown" rather than
//! failing the build over a purely cosmetic feature.

use std::process::Command;

fn main() {
    let revision = std::env::var("GIT_REVISION")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(git_describe)
        .map(|s| shorten_if_full_sha(&s))
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=RUSTHINQ_REVISION={revision}");
    // Best-effort: re-run when HEAD moves to a different commit or branch. Not
    // wired to every ref update (a new tag on the same commit, say), which is fine
    // for a version string nobody depends on being byte-exact on every build.
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-env-changed=GIT_REVISION");
}

fn git_describe() -> Option<String> {
    let output = Command::new("git")
        .args(["describe", "--always", "--dirty", "--tags"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8(output.stdout).ok()?.trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

/// CI passes a full 40-character commit hash via `GIT_REVISION`; shorten it to match
/// what `git describe` prints for everyone else.
fn shorten_if_full_sha(s: &str) -> String {
    if s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        s[..7].to_string()
    } else {
        s.to_string()
    }
}
