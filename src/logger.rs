// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-nexus Authors
// SPDX-License-Identifier: Apache-2.0

//! Engine log severity and the handler that receives log lines.

/// Severity of an engine log line forwarded to a [`LogHandler`].
///
/// The numeric values are part of the FFI contract with the Go bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    /// Diagnostic detail (poll internals, worker lifecycle).
    Debug = 0,
    /// Normal lifecycle events (subscription started, rebalance complete).
    Info = 1,
    /// Recoverable anomalies (poll retry, drain nearing timeout).
    Warn = 2,
    /// Failures (broker errors, circuit breaker, emergency shutdown).
    Error = 3,
}

impl LogLevel {
    /// Map a raw FFI level to a LogLevel. Unknown values map to Info.
    #[doc(hidden)]
    pub fn from_raw(raw: i32) -> Self {
        match raw {
            0 => LogLevel::Debug,
            1 => LogLevel::Info,
            2 => LogLevel::Warn,
            3 => LogLevel::Error,
            _ => LogLevel::Info,
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        };
        f.write_str(s)
    }
}

/// Optional handler receiving engine log lines.
///
/// Registering it routes the engine's internal logging here instead of its
/// default stderr logger. Called from Go runtime threads: implementations
/// must be fast and must not block.
pub trait LogHandler: Send + Sync + 'static {
    /// Receive one engine log line.
    fn log(&self, level: LogLevel, message: &str);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Raw FFI levels 0-3 map to their variants; unknown values map to Info.
    #[test]
    fn log_level_from_raw() {
        assert_eq!(LogLevel::from_raw(0), LogLevel::Debug);
        assert_eq!(LogLevel::from_raw(1), LogLevel::Info);
        assert_eq!(LogLevel::from_raw(2), LogLevel::Warn);
        assert_eq!(LogLevel::from_raw(3), LogLevel::Error);
        assert_eq!(
            LogLevel::from_raw(99),
            LogLevel::Info,
            "unknown maps to Info"
        );
    }

    /// The derived Ord follows severity, so handlers can threshold-filter
    /// with plain comparisons.
    #[test]
    fn log_level_ordering_tracks_severity() {
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
    }

    /// The numeric discriminants are the FFI contract with the Go bridge:
    /// reordering the variants would silently remap every engine log line.
    #[test]
    fn discriminants_pin_the_ffi_contract() {
        assert_eq!(LogLevel::Debug as i32, 0);
        assert_eq!(LogLevel::Info as i32, 1);
        assert_eq!(LogLevel::Warn as i32, 2);
        assert_eq!(LogLevel::Error as i32, 3);
    }

    /// A negative raw value (defensive FFI case) maps to Info like any other
    /// unknown, and Display renders the documented upper-case names.
    #[test]
    fn negative_raw_and_display_names() {
        assert_eq!(LogLevel::from_raw(-1), LogLevel::Info);
        assert_eq!(LogLevel::Debug.to_string(), "DEBUG");
        assert_eq!(LogLevel::Info.to_string(), "INFO");
        assert_eq!(LogLevel::Warn.to_string(), "WARN");
        assert_eq!(LogLevel::Error.to_string(), "ERROR");
    }
}
