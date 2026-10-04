# rusthinq 0.2 Documentation

Start with the [project README](../README.md) for the TS rethink / Rust 0.1 / Rust
0.2 comparison, workspace boundaries and build commands.

## Operating guides

| Document | Contents |
| --- | --- |
| [Runtime and builds](0.2-runtime.md) | Device listeners, certificates, routing, shutdown and release size optimization |
| [Operations](0.2-operations.md) | Management API, LG accounts, bridge, Rhai and external MQTT |
| [Dashboard](0.2-dashboard.md) | Devices, cloud, activity, system and packet analysis |
| [Tools and migration](0.2-tools-migration.md) | CLI/MCP, capture/replay, setup, migration, rollback and release changes |

## Architecture and evidence

| Document | Contents |
| --- | --- |
| [Architecture](0.2-architecture.md) | Design decisions, contracts, ownership review, original plan and provenance |
| [Protocol implementation](0.2-protocol-implementation.md) | L1 protocol, L2 lifecycle, L3 transports, MQTT, provisioning and ThinQ1 HTTP evidence |
| [Runtime implementation](0.2-runtime-implementation.md) | L4 cloud/relay, L5 scripts, L6 application and refactoring evidence |
| [Validation](0.2-validation.md) | Current milestone ledger, compatibility, non-inferiority audit and replay fixtures |
| [Polling audit](0.2-polling-audit.md) | Refresh ownership and remaining periodic work |

Each consolidated record has a contents list and section anchors. Implementation
records retain their development checkpoints; test totals and unresolved items in
older sections describe that checkpoint, not necessarily today's tree. The
[milestone ledger](0.2-validation.md#milestones) owns current acceptance status.
Use the operating guides for configuration.

Local tests do not establish live LG-account or real-appliance compatibility.
Model porting and deployment packaging follow host completion.
