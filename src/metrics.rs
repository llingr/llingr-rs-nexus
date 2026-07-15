// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-nexus Authors
// SPDX-License-Identifier: Apache-2.0

//! Per-message metrics and the handler that receives them.

use crate::Traits;

/// Per-message metrics collected by the processing pipeline.
///
/// Timestamps are Unix epoch nanoseconds; durations are elapsed nanoseconds.
/// Unlike [`Message`](crate::Message), `Metrics` is `Copy` and may be kept
/// beyond the callback. The Go engine's `Metrics.WorkerPool` (an internal
/// worker-pool diagnostic) is deliberately not forwarded over the FFI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metrics {
    /// Combined framework + application trait bits.
    pub traits: Traits,
    /// Number of messages queued for the worker serving this key.
    pub queue_depth: i32,
    /// Kafka partition.
    pub partition: i32,
    /// Offset within the partition.
    pub offset: i64,
    /// Time spent in ProcessMessage callback (nanoseconds).
    pub process_duration_ns: i64,
    /// Time spent in WriteDeadLetter callback (nanoseconds, zero if no error).
    pub deadletter_duration_ns: i64,
    /// Unix epoch nanoseconds when the message was read from the broker.
    pub read_time_ns: i64,
    /// Unix epoch nanoseconds when processing started.
    pub process_start_time_ns: i64,
    /// Unix epoch nanoseconds when the offset watermark advanced.
    pub watermark_advance_time_ns: i64,
}

/// Optional handler receiving per-message metrics from the pipeline.
///
/// Called once per message: implementations must be fast and non-blocking.
/// Unlike Go's `MetricsSink`, no per-call identity (topic, consumer group,
/// service) accompanies the packet: a binding consumer serves exactly one
/// topic and group, so that identity is static and already known.
pub trait MetricsHandler: Send + Sync + 'static {
    /// Receive one message's metrics packet.
    fn handle(&self, metrics: &Metrics);
}
