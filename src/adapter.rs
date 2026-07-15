// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-nexus Authors
// SPDX-License-Identifier: Apache-2.0

//! Adapter selection and options vocabulary.

/// Broker adapter compiled into the Go shared library.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Adapter {
    /// franz-go: pure Go Kafka client, no librdkafka. The default.
    #[default]
    Franz,
    /// confluent-kafka-go: librdkafka (statically linked into the shared
    /// library). Exposes the full librdkafka configuration surface.
    Kafka,
}

impl Adapter {
    /// The adapter name understood by the Go bridge's config JSON.
    pub fn as_str(&self) -> &'static str {
        match self {
            Adapter::Franz => "franz",
            Adapter::Kafka => "kafka",
        }
    }
}

/// How the consumer starts when no committed offset exists for a partition
/// (both adapters' `auto.offset.reset` option).
///
/// librdkafka's third value `error` (fail instead of resetting) has no
/// franz-go equivalent, so it is not typed here; on the kafka adapter use
/// `KafkaOptions::set("auto.offset.reset", "error")`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoOffsetReset {
    /// Start from the oldest available offset.
    Earliest,
    /// Start from the next offset produced.
    Latest,
}

impl AutoOffsetReset {
    /// The option value understood by both adapters.
    pub fn as_str(&self) -> &'static str {
        match self {
            AutoOffsetReset::Earliest => "earliest",
            AutoOffsetReset::Latest => "latest",
        }
    }
}

/// Typed, adapter-specific Kafka client options.
///
/// Implemented by `FranzOptions` (llingr-adapter-franz) and `KafkaOptions`
/// (llingr-adapter-kafka); applied with `BrokerConfig::adapter_options`,
/// which selects the adapter and its options together so they cannot be
/// mismatched.
pub trait AdapterOptions {
    /// The adapter these options target.
    fn adapter(&self) -> Adapter;
    /// The option entries as Kafka-style key/value pairs.
    fn entries(&self) -> Vec<(String, String)>;
    /// Configuration errors detectable client-side, such as the same
    /// security key arriving from both a typed setter and a raw pair;
    /// surfaced as a clean error at engine build time.
    fn validate(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Whole milliseconds for the wire format, rounding a sub-millisecond
/// remainder UP so a non-zero `Duration` never silently becomes zero
/// (downstream, zero means "use the engine default"). Public only for the
/// config and adapter builders.
#[doc(hidden)]
pub fn duration_ms_ceil(d: std::time::Duration) -> u128 {
    d.as_nanos().div_ceil(1_000_000)
}

/// References to options are options: lets `adapter_options` take
/// `impl AdapterOptions` by value while `&FranzOptions` / `&dyn AdapterOptions`
/// call sites keep working.
impl<T: AdapterOptions + ?Sized> AdapterOptions for &T {
    fn adapter(&self) -> Adapter {
        (**self).adapter()
    }
    fn entries(&self) -> Vec<(String, String)> {
        (**self).entries()
    }
    fn validate(&self) -> Result<(), String> {
        (**self).validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wire names the Go bridge parses ("franz"/"kafka",
    /// "earliest"/"latest") and the franz default.
    #[test]
    fn adapter_and_offset_reset_as_str() {
        assert_eq!(Adapter::Franz.as_str(), "franz");
        assert_eq!(Adapter::Kafka.as_str(), "kafka");
        assert_eq!(Adapter::default(), Adapter::Franz);
        assert_eq!(AutoOffsetReset::Earliest.as_str(), "earliest");
        assert_eq!(AutoOffsetReset::Latest.as_str(), "latest");
    }

    /// Sub-millisecond remainders round UP: a non-zero Duration must never
    /// serialize as 0ms, which the engine would read as "use the default".
    #[test]
    fn duration_ms_ceil_never_silently_zero() {
        use std::time::Duration;
        assert_eq!(duration_ms_ceil(Duration::ZERO), 0);
        assert_eq!(
            duration_ms_ceil(Duration::from_micros(500)),
            1,
            "sub-ms rounds UP"
        );
        assert_eq!(duration_ms_ceil(Duration::from_millis(1)), 1);
        assert_eq!(duration_ms_ceil(Duration::from_micros(1500)), 2);
        assert_eq!(duration_ms_ceil(Duration::from_secs(30)), 30_000);
        // The true ceil boundaries: 1ns (smallest non-zero) and 999999ns
        // (just below 1ms) both round up to exactly 1, never 0.
        assert_eq!(duration_ms_ceil(Duration::from_nanos(1)), 1);
        assert_eq!(duration_ms_ceil(Duration::from_nanos(999_999)), 1);
        assert_eq!(duration_ms_ceil(Duration::from_nanos(1_000_001)), 2);
    }

    /// The blanket `impl AdapterOptions for &T` forwards all three methods,
    /// and `validate` defaults to Ok for implementations that do not override
    /// it. This is what lets `adapter_options(&opts)` call sites work.
    #[test]
    fn reference_blanket_impl_forwards_and_validate_defaults_ok() {
        struct Opts;
        impl AdapterOptions for Opts {
            fn adapter(&self) -> Adapter {
                Adapter::Kafka
            }
            fn entries(&self) -> Vec<(String, String)> {
                vec![("client.id".to_string(), "x".to_string())]
            }
        }

        let by_ref: &Opts = &Opts;
        assert_eq!(by_ref.adapter(), Adapter::Kafka);
        assert_eq!(by_ref.entries(), Opts.entries());
        assert!(by_ref.validate().is_ok(), "default validate forwards Ok");

        let as_dyn: &dyn AdapterOptions = &Opts;
        assert_eq!(as_dyn.adapter(), Adapter::Kafka);
        assert!(as_dyn.validate().is_ok());
    }
}
