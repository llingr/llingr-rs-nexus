// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-nexus Authors
// SPDX-License-Identifier: Apache-2.0

//! Consumer lifecycle handler.

/// Optional handler for consumer shutdown notification.
pub trait ShutdownHandler: Send + Sync + 'static {
    /// Called once when the consumer exits; `reason` is
    /// "graceful shutdown" or the engine's failure description.
    fn handle(&self, reason: &str);
}
