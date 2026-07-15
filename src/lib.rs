// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-nexus Authors
// SPDX-License-Identifier: Apache-2.0

//! Contracts for the llingr Rust message-processing ecosystem, mirroring the
//! Go `llingr-nexus` module: the message and metrics types delivered to
//! application code, the trait-bit field, and the handler traits an
//! application implements.
//!
//! Pure data and trait definitions: no dependencies, no FFI. Adapters,
//! loggers, and the engine binding all link through this crate, and the
//! public API is flat: everything is re-exported at the root, so
//! applications write `use llingr_nexus::Message` and friends.

#![deny(missing_docs)]

mod adapter;
mod bandwidth;
mod consumer;
mod logger;
mod message;
mod metrics;
mod traits;

pub use adapter::{Adapter, AdapterOptions, AutoOffsetReset};
pub use bandwidth::{BandwidthMetrics, BandwidthMetricsHandler, BrokerInfo, PartitionBandwidth};
pub use consumer::ShutdownHandler;
pub use logger::{LogHandler, LogLevel};
pub use message::{DeadLetterHandler, Header, Headers, Message, ProcessHandler, Timestamp};
pub use metrics::{Metrics, MetricsHandler};
pub use traits::Traits;

// Internal helper shared with the adapter/facade crates; not application API.
#[doc(hidden)]
pub use adapter::duration_ms_ceil;
