# llingr-nexus

Zero-dependency Rust contract types for **llingr**, a concurrent, ordered
message-processing engine for Kafka and RedPanda. Learn more at
[llingr.io](https://llingr.io).

This crate is the shared vocabulary the rest of the Rust ecosystem is built on: the
message and metrics types your code receives, the trait-bit field, and the handler traits
you implement. It has **no dependencies and no FFI**: adapters, loggers, applications, and
the engine binding all link through it.

- **Message data**: `Message` (borrowed key/value, topic, partition, offset),
  `Timestamp`, `Header`/`Headers`
- **Trait bit field**: `Traits` (framework bits 0-9, application bits 10-63)
- **Per-message telemetry**: `Metrics`
- **Bandwidth telemetry**: `BandwidthMetrics`, `BrokerInfo`, `PartitionBandwidth`
- **Engine log lines**: `LogLevel`
- **Handler traits**: `ProcessHandler`, `DeadLetterHandler`, `MetricsHandler`,
  `BandwidthMetricsHandler`, `ShutdownHandler`, `LogHandler`
- **Adapter vocabulary**: `Adapter`, `AutoOffsetReset`, `AdapterOptions`

Handler semantics (ordering guarantees, the error and dead-letter contract, threading
rules) are documented on the trait definitions themselves, browse them on
[docs.rs](https://docs.rs/llingr-nexus).

Applications consuming messages want the [`llingr`](https://crates.io/crates/llingr)
crate, which re-exports these types alongside the engine. Because `llingr-nexus` is
dependency-free, a custom adapter, tooling, or a test harness can instead build against
the contracts directly without pulling in the engine.

## Licence

Apache-2.0 - see [LICENSE](./LICENSE) and [COPYRIGHT](./COPYRIGHT).

The contracts are Apache-2.0 and will remain Apache-2.0: applications and tooling can
depend on this crate without copyleft obligations.
