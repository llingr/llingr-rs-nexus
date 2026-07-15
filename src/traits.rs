// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-nexus Authors
// SPDX-License-Identifier: Apache-2.0

/// The 64-bit trait bit field.
///
/// Bits 0-9 are framework-reserved (read via the `has_*` getters); bits
/// 10-63 are application flags. Attempts to set reserved bits are silently
/// masked, not errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Traits(u64);

/// Mask covering framework-reserved bits 0-9.
const FRAMEWORK_RESERVED: u64 = 0x3FF;

impl Traits {
    /// No application traits set.
    pub fn none() -> Self {
        Self(0)
    }

    /// Reconstruct from a raw value received over the FFI boundary.
    /// Intended for the binding layer, not application code.
    #[doc(hidden)]
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Create traits with the given bit position set (must be >= 10).
    /// Positions below 10 are silently ignored.
    ///
    /// # Example
    ///
    /// ```
    /// use llingr_nexus::Traits;
    ///
    /// const AUDITED: u32 = 10;
    /// const HIGH_VALUE: u32 = 20;
    ///
    /// let t = Traits::with_bit(AUDITED).set(HIGH_VALUE);
    /// assert!(t.has(AUDITED) && t.has(HIGH_VALUE));
    ///
    /// // Bits 0-9 are framework-reserved: silently ignored.
    /// assert_eq!(Traits::with_bit(5).raw(), 0);
    /// ```
    pub fn with_bit(bit: u32) -> Self {
        if !(10..=63).contains(&bit) {
            return Self(0);
        }
        Self(1u64 << bit)
    }

    /// Set a bit position (must be >= 10). Returns self for chaining.
    pub fn set(mut self, bit: u32) -> Self {
        if (10..=63).contains(&bit) {
            self.0 |= 1u64 << bit;
        }
        self
    }

    /// Check whether a bit position is set.
    pub fn has(&self, bit: u32) -> bool {
        if bit > 63 {
            return false;
        }
        self.0 & (1u64 << bit) != 0
    }

    /// The raw u64 value with framework bits masked out.
    pub fn raw(&self) -> u64 {
        self.0 & !FRAMEWORK_RESERVED
    }

    /// The raw u64 including framework bits (for metrics inspection).
    pub fn raw_with_framework(&self) -> u64 {
        self.0
    }

    /// Whether the ProcessError flag is set (bit 0): the process handler
    /// returned an error.
    pub fn has_process_error(&self) -> bool {
        self.0 & (1 << 0) != 0
    }

    /// Whether the ProcessPanic flag is set (bit 1): the Go engine recovered
    /// a panic from its process callback. A Rust handler panic is caught at
    /// the FFI boundary and surfaces as ProcessError (bit 0) instead; this
    /// bit firing means a bug in the bridge itself.
    pub fn has_process_panic(&self) -> bool {
        self.0 & (1 << 1) != 0
    }

    /// Whether the DeadLetter flag is set (bit 2): the message was routed to
    /// the dead-letter handler.
    pub fn has_dead_letter(&self) -> bool {
        self.0 & (1 << 2) != 0
    }

    /// Whether the CommitBuffered flag is set (bit 3): the offset was buffered
    /// by the gap-buffer committer rather than committed immediately.
    pub fn has_commit_buffered(&self) -> bool {
        self.0 & (1 << 3) != 0
    }

    /// Whether the Duplicate flag is set (bit 4): the message was redelivered.
    pub fn has_duplicate(&self) -> bool {
        self.0 & (1 << 4) != 0
    }

    /// Whether the UsedOverflow flag is set (bit 5).
    pub fn has_used_overflow(&self) -> bool {
        self.0 & (1 << 5) != 0
    }

    /// Whether the Orphaned flag is set (bit 6): the work item was orphaned by
    /// a rebalance.
    pub fn has_orphaned(&self) -> bool {
        self.0 & (1 << 6) != 0
    }

    /// Whether the FirstAfterRebalance flag is set (bit 7): the first message
    /// processed on its partition after a rebalance assignment.
    pub fn has_first_after_rebalance(&self) -> bool {
        self.0 & (1 << 7) != 0
    }
}

impl std::ops::BitOr for Traits {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bits set via with_bit/set/BitOr read back with has; raw() strips the
    /// framework range; positions outside 10-63 are ignored.
    #[test]
    fn traits_round_trip() {
        let t = Traits::with_bit(10).set(20).set(63);
        assert!(t.has(10));
        assert!(t.has(20));
        assert!(t.has(63));
        assert!(!t.has(9));
        assert!(!t.has(11));

        let raw = t.raw();
        assert_eq!(raw & 0x3FF, 0, "framework bits masked in raw()");
        assert_ne!(raw, 0);

        assert_eq!(t.raw_with_framework() & !0x3FF, raw);

        let none = Traits::none();
        assert_eq!(none.raw(), 0);
        assert_eq!(none.raw_with_framework(), 0);

        let combined = Traits::with_bit(10) | Traits::with_bit(20);
        assert!(combined.has(10));
        assert!(combined.has(20));

        assert_eq!(Traits::with_bit(64).raw(), 0, "out of range ignored");
        assert_eq!(Traits::with_bit(5).raw(), 0, "framework range ignored");
    }

    /// `has` guards bit positions above 63: without the guard the shift
    /// overflows (a panic in debug builds). Must answer false, never panic.
    #[test]
    fn has_out_of_range_bit_is_false_not_panic() {
        let t = Traits::from_raw(u64::MAX);
        assert!(!t.has(64));
        assert!(!t.has(u32::MAX));
        assert!(t.has(63), "in-range top bit still readable");
    }

    /// `set` has its own range guard, separate from `with_bit`: framework
    /// bits (0-9) and out-of-range bits are silently ignored.
    #[test]
    fn set_ignores_framework_and_out_of_range_bits() {
        let t = Traits::none().set(9).set(64);
        assert_eq!(t.raw_with_framework(), 0, "set(9)/set(64) must be no-ops");
        assert_eq!(
            Traits::with_bit(9).raw_with_framework(),
            0,
            "with_bit(9) below app range"
        );
        let boundary = Traits::none().set(10);
        assert!(boundary.has(10), "bit 10 is the first application bit");
    }

    /// raw() masks framework bits back OUT of a value that arrived over the
    /// FFI with them present.
    #[test]
    fn raw_masks_framework_bits_from_raw_values() {
        let t = Traits::from_raw(0x3FF | (1 << 10));
        assert_eq!(t.raw(), 1 << 10, "framework bits stripped");
        assert_eq!(t.raw_with_framework(), 0x3FF | (1 << 10));
    }

    /// Each framework-flag getter reads its own bit (0-7) from an FFI-origin
    /// raw value: a shifted getter would misreport every engine flag.
    #[test]
    fn framework_flags_from_raw() {
        let t = Traits::from_raw(0b1111_1111);
        assert!(t.has_process_error());
        assert!(t.has_process_panic());
        assert!(t.has_dead_letter());
        assert!(t.has_commit_buffered());
        assert!(t.has_duplicate());
        assert!(t.has_used_overflow());
        assert!(t.has_orphaned());
        assert!(t.has_first_after_rebalance());

        let none = Traits::none();
        assert!(!none.has_process_error());
        assert!(!none.has_first_after_rebalance());
    }
}
