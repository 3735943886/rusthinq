# rusthinq 0.1 — Fixed Reference

This directory is the complete tracked 0.1 project, relocated unchanged except for the
adjustments listed below (see [D0](../../docs/0.2-design.md)). It is a behavioral and protocol
reference only: no 0.2 crate may depend on it by path, include its source, or fall back to it.
It is removed after the 0.2 release gates pass (M5).

- Original revision: `2473c34683130c29ad2f07d560d15b2c94342a36` (master, before relocation)
- Workspace: independent; not a member of the root workspace and not built by root CI.

## Build and Run

Run from this directory:

```sh
cd legacy/0.1
cargo build --workspace
cargo test --workspace
cargo run -p rusthinq-cloud --features bridge,scripting -- ./config.toml
```

## Relocation Adjustments

- `crates/rusthinq-devices/tests/il_vectors.rs`: the sibling `ildevice` path moved from
  `../../../ildevice` to `../../../../../ildevice` (the test skips silently when not found).
- `.github/`: archived here with the project; GitHub does not run workflows from this location,
  so 0.1 CI, release, driver, dependabot, and upstream-sync automation are inactive on this branch.
- `.gitignore`: archived here; it applies only to this subtree. The root has its own `.gitignore`.
