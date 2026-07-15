// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-nexus Authors
// SPDX-License-Identifier: Apache-2.0

//! Bandwidth telemetry packets and the handler that receives them.

/// One bandwidth telemetry packet (Go `nexus.BandwidthMetrics`): cumulative
/// per-partition byte counts over one collection interval, plus the broker
/// topology they were measured against.
///
/// Partition keys and payloads are excluded by design (the engine's
/// compliance guarantee: infrastructure-level data only). Unlike
/// [`Message`](crate::Message), the packet owns its data and arrives off the
/// message hot path, so it may be stored or forwarded freely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BandwidthMetrics {
    /// Measurement timestamp, Unix epoch nanoseconds.
    pub ts_unix_ns: i64,
    /// The adapter's collection cadence for this packet, in milliseconds.
    pub stats_interval_ms: i64,
    /// Idempotency ID for the packet (typically a UUID).
    pub metrics_id: String,
    /// Topic the packet describes.
    pub topic: String,
    /// Consumer group the packet describes.
    pub consumer_group: String,
    /// Broker topology at measurement time.
    pub brokers: Vec<BrokerInfo>,
    /// Per-partition counters.
    pub partitions: Vec<PartitionBandwidth>,
}

/// A broker node at measurement time (Go `nexus.BrokerInfo`). Identifiers
/// are strings for forward compatibility across broker systems.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    /// Broker node ID.
    pub id: String,
    /// Broker host.
    pub host: String,
    /// Broker port.
    pub port: String,
    /// Availability zone or rack; empty when the broker/adapter does not
    /// supply one.
    pub rack: String,
}

/// Cumulative bandwidth counters for one partition over one collection
/// interval (Go `nexus.PartitionBandwidth`). Compression fields are zero
/// or empty when the client adapter has no compression visibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionBandwidth {
    /// Measurement timestamp, Unix epoch nanoseconds.
    pub ts_unix_ns: i64,
    /// Cumulative bytes received.
    pub received_bytes: i64,
    /// Cumulative bytes transmitted.
    pub transmitted_bytes: i64,
    /// Cumulative messages received.
    pub received_message_count: i64,
    /// Wire bytes (zero if unavailable).
    pub compressed_bytes: i64,
    /// Decompressed bytes (zero if unavailable).
    pub uncompressed_bytes: i64,
    /// Partition identifier; aligns with [`Metrics::partition`](crate::Metrics::partition).
    pub id: i32,
    /// Broker serving this partition.
    pub leader: String,
    /// Compression algorithm name (empty if unavailable).
    pub compression: String,
}

/// Optional handler receiving bandwidth telemetry packets (Go's
/// `WithBandwidthMetricsSink`).
///
/// Registering it is what enables collection: the broker adapter gathers
/// counters at its stats interval and the engine's aggregator delivers a
/// packet here on each flush.
pub trait BandwidthMetricsHandler: Send + Sync + 'static {
    /// Receive one flushed bandwidth packet.
    fn handle(&self, metrics: &BandwidthMetrics);
}
