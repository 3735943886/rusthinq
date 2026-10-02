# rusthinq 0.2

The workspace contains the independent 0.2 runtime. The fixed 0.1 reference is
archived in `legacy/0.1`; migration is still in progress.

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p rusthinq-app --bin rusthinq -- /path/to/config.toml
```

Configure TLS device endpoints, an existing CA and durable device inventory using
[the runtime guide](docs/0.2-runtime.md). Optional management, real Rhai drivers and
external MQTT are documented in [operations](docs/0.2-operations.md).
The management API and dashboard work independently of the external broker.

[Milestones](docs/0.2-milestones.md) record completed evidence and remaining work.
Real-appliance/LG-account validation is excluded at the user's request; unfinished
implementation items remain visible and are not recorded as completed validation.
