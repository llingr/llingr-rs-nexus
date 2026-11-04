// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-nexus Authors
// SPDX-License-Identifier: Apache-2.0

//! Consumer lifecycle contracts.

/// Optional handler for consumer shutdown notification.
pub trait ShutdownHandler: Send + Sync + 'static {
    /// Called once when the consumer exits; `reason` is
    /// "graceful shutdown" or the engine's failure description.
    fn handle(&self, reason: &str);
}

/// Stops a consumer without draining on an irrecoverable, typically
/// infrastructure, failure: the Rust form of the Go nexus
/// `EmergencyShutdowner` contract.
///
/// Nothing waits for in-flight work. Work that completes as the stop takes
/// effect may still be committed; work that has not completed is redelivered
/// after a restart. The registered [`ShutdownHandler`] is notified exactly
/// once, with a description that includes `reason`.
///
/// Implementations accept a call from any thread and at any point in the
/// lifecycle: before consumption starts, during assignment, while running and
/// while shutting down. Calls after the first are no-ops.
///
/// Code that holds the consumer only through this trait can escalate without
/// depending on the engine crate:
///
/// ```
/// use std::sync::Arc;
/// use llingr_nexus::EmergencyStop;
///
/// fn on_storage_lost(consumer: &Arc<dyn EmergencyStop>) {
///     consumer.emergency_stop("storage unreachable");
/// }
/// ```
pub trait EmergencyStop: Send + Sync {
    /// Abandon in-flight work and stop now, reporting `reason`.
    fn emergency_stop(&self, reason: &str);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Callers share the consumer across threads as `Arc<dyn EmergencyStop>`,
    /// which needs the trait to stay object-safe with Send + Sync supertraits.
    #[test]
    fn emergency_stop_trait_object_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync + ?Sized>() {}
        assert_send_sync::<dyn EmergencyStop>();
    }
}
