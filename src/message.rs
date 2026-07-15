// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-nexus Authors
// SPDX-License-Identifier: Apache-2.0

//! The complete record delivered to application code, its timestamp and header
//! views, and the two required handlers.

use std::fmt;

use crate::Traits;

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

/// A record's timestamp and how it was set: one enum carries the instant and
/// its kind (rdkafka's shape), so there is no separate "type" field to
/// juggle. Milliseconds since the Unix epoch (the Kafka wire resolution);
/// the named `millis` field keeps the unit visible at match sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timestamp {
    /// The record carried no usable timestamp.
    NotAvailable,
    /// Producer-assigned event time.
    CreateTime {
        /// Milliseconds since the Unix epoch.
        millis: i64,
    },
    /// Broker-assigned ingestion time.
    LogAppendTime {
        /// Milliseconds since the Unix epoch.
        millis: i64,
    },
}

impl Timestamp {
    /// The millisecond value if either kind is present, else `None`.
    pub fn millis(self) -> Option<i64> {
        match self {
            Timestamp::NotAvailable => None,
            Timestamp::CreateTime { millis } | Timestamp::LogAppendTime { millis } => Some(millis),
        }
    }
}

// ---------------------------------------------------------------------------
// Headers - an ordered, borrowed view. Kafka headers are a LIST, not a map:
// keys may repeat and order is preserved (tracing systems depend on it), and
// values are nullable. So we never collapse them into a HashMap.
// ---------------------------------------------------------------------------

/// One header: a UTF-8 key and an optional (nullable) byte value.
#[derive(Debug, Clone, Copy)]
pub struct Header<'a> {
    /// Header key.
    pub key: &'a str,
    /// Header value, or `None` for a null-valued header.
    pub value: Option<&'a [u8]>,
}

/// Ordered, borrowed view over a record's headers.
#[derive(Clone, Copy)]
pub struct Headers<'a> {
    entries: &'a [Header<'a>],
}

impl<'a> Headers<'a> {
    /// Wrap a borrowed header slice (the binding assembles this per message).
    pub fn from_slice(entries: &'a [Header<'a>]) -> Self {
        Self { entries }
    }

    /// Number of headers (duplicates counted separately).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there are no headers.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The header at `index` in wire order.
    pub fn get(&self, index: usize) -> Option<Header<'a>> {
        self.entries.get(index).copied()
    }

    /// The first header matching `key` (headers may repeat; matches the usual
    /// Kafka-client "first wins" convention).
    pub fn find(&self, key: &str) -> Option<Header<'a>> {
        self.entries.iter().copied().find(|h| h.key == key)
    }

    /// Iterate headers in wire order.
    pub fn iter(&self) -> impl Iterator<Item = Header<'a>> + '_ {
        self.entries.iter().copied()
    }
}

// ---------------------------------------------------------------------------
// Message
//
// repr(C) fixes field order (repr(Rust) reorders for size); fields are
// private so the layout stays an implementation detail. Declaration order is
// hot to cold: key/value/headers/offset/partition on cache line 0, timestamp
// and topic on line 1. Exact offsets are pinned by message_cache_line_layout.
// ---------------------------------------------------------------------------

/// A complete record delivered to the process callback.
///
/// Borrows all its data from the FFI boundary: valid only for the duration of
/// the callback invocation. Do not store references to the key, value, or
/// header slices beyond the callback return; copy out (`to_vec()`,
/// `to_owned()`) anything you keep longer.
#[repr(C)]
pub struct Message<'a> {
    // Cache line 0 - hot path.
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
    headers: Headers<'a>,
    offset: i64,
    partition: i32,

    // Cache line 1 - read on demand.
    timestamp: Timestamp,
    topic: &'a str,
}

impl<'a> Message<'a> {
    /// Assemble a message from its borrowed parts (binding layer, not app code).
    ///
    /// Safe: `key`/`value` are byte slices, so there is no UTF-8 precondition
    /// and no `from_utf8_unchecked` footgun; [`key_str`](Self::key_str) does a
    /// checked conversion on demand.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key: Option<&'a [u8]>,
        value: Option<&'a [u8]>,
        topic: &'a str,
        partition: i32,
        offset: i64,
        timestamp: Timestamp,
        headers: Headers<'a>,
    ) -> Self {
        Self {
            key,
            value,
            headers,
            offset,
            partition,
            timestamp,
            topic,
        }
    }

    // rdkafka-shaped accessors.

    /// Partition key bytes, or `None` for a keyless record.
    pub fn key(&self) -> Option<&'a [u8]> {
        self.key
    }

    /// Partition key as a UTF-8 string, or `None` for a keyless or (in
    /// principle) non-UTF-8 key. The broker adapters deliver UTF-8-safe keys
    /// (raw if valid, base64 if binary, partition number if absent), so on
    /// adapter-delivered records this is always `Some`; [`key`](Self::key)
    /// has the raw bytes.
    pub fn key_str(&self) -> Option<&'a str> {
        self.key.and_then(|k| std::str::from_utf8(k).ok())
    }

    /// Record value (rdkafka calls this `payload`), or `None` for a null value
    /// such as a tombstone.
    pub fn payload(&self) -> Option<&'a [u8]> {
        self.value
    }

    /// Alias for [`payload`](Self::payload), matching the Go/`nexus` naming.
    pub fn value(&self) -> Option<&'a [u8]> {
        self.value
    }

    /// The value as a UTF-8 string, or `None` when it is null or not valid
    /// UTF-8 (values are frequently binary; use [`value`](Self::value) for the
    /// raw bytes).
    pub fn value_str(&self) -> Option<&'a str> {
        self.value.and_then(|v| std::str::from_utf8(v).ok())
    }

    /// The topic this record came from.
    pub fn topic(&self) -> &'a str {
        self.topic
    }

    /// Partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Offset within the partition.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Record timestamp and its kind.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// The record's headers, in wire order.
    pub fn headers(&self) -> Headers<'a> {
        self.headers
    }
}

/// Manual Debug: lengths and coordinates only. Keys are frequently PII and
/// values can be large or binary, so contents are never printed.
impl fmt::Debug for Message<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("key_len", &self.key.map(<[u8]>::len))
            .field("value_len", &self.value.map(<[u8]>::len))
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .field("timestamp", &self.timestamp)
            .field("header_count", &self.headers.len())
            .field("topic", &self.topic)
            .finish()
    }
}

/// Required handler processing each message.
///
/// Invoked from a worker goroutine, with per-key ordering guaranteed.
/// Required together with [`DeadLetterHandler`].
///
/// # Traits
///
/// Custom trait bits are the RETURN value, not a field on the message: the
/// message is a read-only view of the record, and the traits an application
/// attaches (business-intelligence flags, bits 10-63) are an output of
/// processing. Return [`Traits::none`] when there is nothing to attach.
///
/// # Errors
///
/// Returning an error routes the message to the dead-letter handler
/// (if registered) and sets the ProcessError framework trait.
///
/// # Example
///
/// ```
/// use llingr_nexus::{Message, ProcessHandler, Traits};
///
/// struct OrderProcessor;
///
/// impl ProcessHandler for OrderProcessor {
///     fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>> {
///         // A null value is a tombstone; this handler treats it as a failure
///         // (a real one might route deletes instead).
///         let value = msg.value().ok_or("unexpected tombstone")?;
///         if value.is_empty() {
///             return Err("empty order payload".into());
///         }
///         // Attach an application trait bit (10-63) as the return value.
///         Ok(Traits::with_bit(10))
///     }
/// }
/// ```
pub trait ProcessHandler: Send + Sync + 'static {
    /// Process one message. Return application trait bits on success; an
    /// error routes the message to the dead-letter handler.
    fn process(&self, msg: &Message) -> Result<Traits, Box<dyn std::error::Error>>;
}

/// Required handler for messages that failed processing.
///
/// Invoked when `ProcessHandler::process` returns an error (or panics): a
/// failed message must have somewhere to go before its offset commits, or
/// it would be silently dropped. Logging the reason is the bare minimum;
/// publishing to a real dead-letter store is the recommended implementation.
pub trait DeadLetterHandler: Send + Sync + 'static {
    /// Persist or report a failed message; `error_msg` is the process
    /// handler's error text. An error here is counted but not retried.
    fn handle(&self, msg: &Message, error_msg: &str) -> Result<(), Box<dyn std::error::Error>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // The cache-line intent, pinned. 64-bit only (fat pointers are 8 bytes).
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn message_cache_line_layout() {
        use std::mem::{align_of, offset_of, size_of};

        // Hot fields live at the front of cache line 0.
        assert_eq!(offset_of!(Message, key), 0);
        assert_eq!(offset_of!(Message, value), 16);
        assert_eq!(offset_of!(Message, headers), 32);
        assert_eq!(offset_of!(Message, offset), 48);
        assert_eq!(offset_of!(Message, partition), 56);
        assert!(offset_of!(Message, partition) < 64, "record fields on CL0");

        // Extras spill onto cache line 1.
        assert!(offset_of!(Message, timestamp) >= 64, "timestamp on CL1");
        assert!(offset_of!(Message, topic) >= 64, "topic on CL1");

        assert_eq!(size_of::<Message>(), 96);
        assert_eq!(align_of::<Message>(), 8);
    }

    /// Every accessor returns what new() was given, and Debug prints lengths
    /// and coordinates only: keys are frequently PII, values can be binary.
    #[test]
    fn accessors_and_redacted_debug() {
        let headers = [
            Header {
                key: "trace-id",
                value: Some(b"abc123"),
            },
            Header {
                key: "content-type",
                value: Some(b"application/json"),
            },
        ];
        let msg = Message::new(
            Some(b"user-42"),
            Some(b"{\"amount\":100}"),
            "payments",
            3,
            9001,
            Timestamp::CreateTime {
                millis: 1_700_000_000_000,
            },
            Headers::from_slice(&headers),
        );

        assert_eq!(msg.key(), Some(&b"user-42"[..]));
        assert_eq!(msg.key_str(), Some("user-42"));
        assert_eq!(msg.payload(), msg.value());
        assert_eq!(msg.value_str(), Some("{\"amount\":100}"));
        assert_eq!(msg.topic(), "payments");
        assert_eq!(msg.partition(), 3);
        assert_eq!(msg.offset(), 9001);
        assert_eq!(msg.timestamp().millis(), Some(1_700_000_000_000));
        assert_eq!(msg.headers().len(), 2);
        assert_eq!(
            msg.headers().find("trace-id").unwrap().value,
            Some(&b"abc123"[..])
        );
        assert!(msg.headers().find("absent").is_none());

        // Debug exposes lengths, never contents.
        let debug = format!("{msg:?}");
        assert!(!debug.contains("user-42"), "key leaked: {debug}");
        assert!(!debug.contains("amount"), "value leaked: {debug}");
        assert!(debug.contains("header_count"));
    }

    /// The all-None record (keyless, null value, no timestamp, no headers):
    /// every accessor answers None or empty, nothing panics.
    #[test]
    fn null_key_and_value() {
        let msg = Message::new(
            None,
            None,
            "t",
            0,
            0,
            Timestamp::NotAvailable,
            Headers::from_slice(&[]),
        );
        assert_eq!(msg.key(), None);
        assert_eq!(msg.key_str(), None);
        assert_eq!(msg.value(), None);
        assert_eq!(msg.value_str(), None);
        assert_eq!(msg.timestamp().millis(), None);
        assert!(msg.headers().is_empty());
    }

    /// Kafka headers are a LIST: keys repeat, order is wire order, values are
    /// nullable. `find` returns the FIRST match (the usual client convention),
    /// `len` counts duplicates separately, `get`/`iter` preserve wire order.
    #[test]
    fn headers_are_an_ordered_list_with_duplicates_and_null_values() {
        let entries = [
            Header {
                key: "retry",
                value: Some(b"1"),
            },
            Header {
                key: "tombstone-marker",
                value: None,
            },
            Header {
                key: "retry",
                value: Some(b"2"),
            },
        ];
        let headers = Headers::from_slice(&entries);

        assert_eq!(headers.len(), 3, "duplicates counted separately");
        assert_eq!(
            headers.find("retry").unwrap().value,
            Some(&b"1"[..]),
            "find returns the FIRST match, not the last"
        );
        assert_eq!(
            headers.find("tombstone-marker").unwrap().value,
            None,
            "null-valued header"
        );

        // get: wire order, out-of-bounds is None.
        assert_eq!(headers.get(0).unwrap().key, "retry");
        assert_eq!(headers.get(1).unwrap().key, "tombstone-marker");
        assert_eq!(headers.get(2).unwrap().value, Some(&b"2"[..]));
        assert!(headers.get(3).is_none());

        // iter: wire order, all entries.
        let keys: Vec<&str> = headers.iter().map(|h| h.key).collect();
        assert_eq!(keys, ["retry", "tombstone-marker", "retry"]);
    }

    /// Empty is not absent: a zero-length key or value is `Some` (and the
    /// _str accessors give `Some("")`), distinct from the `None` of a keyless
    /// record or a null (tombstone) value. Delete handling on compacted
    /// topics rides exactly this distinction.
    #[test]
    fn empty_key_and_value_stay_some_distinct_from_none() {
        let msg = Message::new(
            Some(b""),
            Some(b""),
            "t",
            0,
            0,
            Timestamp::NotAvailable,
            Headers::from_slice(&[]),
        );
        assert_eq!(msg.key(), Some(&b""[..]), "empty key is Some, not None");
        assert_eq!(msg.key_str(), Some(""), "empty key decodes to empty str");
        assert_eq!(msg.value(), Some(&b""[..]), "empty value is Some, not None");
        assert_eq!(msg.value_str(), Some(""));
        assert_ne!(
            msg.value(),
            None::<&[u8]>,
            "empty and null must not collapse"
        );
    }

    /// `find` returns the FIRST duplicate even when it is null-valued: a
    /// caller doing `find(k).and_then(|h| h.value)` gets `None` although a
    /// later duplicate carries a value. That is the intended first-wins
    /// convention; this pins the null-first variant callers trip on.
    #[test]
    fn find_first_wins_even_when_first_duplicate_is_null() {
        let entries = [
            Header {
                key: "retry",
                value: None,
            },
            Header {
                key: "retry",
                value: Some(b"2"),
            },
        ];
        let headers = Headers::from_slice(&entries);
        let first = headers.find("retry").expect("key present");
        assert_eq!(first.value, None, "the null-valued FIRST entry wins");
        assert_eq!(
            headers.get(1).unwrap().value,
            Some(&b"2"[..]),
            "the valued duplicate is still reachable by position"
        );
    }

    /// The redacted Debug must also render a null-key/null-value message
    /// cleanly (lengths show as None, nothing to leak, no panic).
    #[test]
    fn redacted_debug_renders_null_message() {
        let msg = Message::new(
            None,
            None,
            "t",
            0,
            0,
            Timestamp::NotAvailable,
            Headers::from_slice(&[]),
        );
        let debug = format!("{msg:?}");
        assert!(debug.contains("key_len: None"), "{debug}");
        assert!(debug.contains("value_len: None"), "{debug}");
        assert!(debug.contains("header_count: 0"), "{debug}");
    }

    /// Broker-assigned LogAppendTime is a distinct kind from CreateTime, and
    /// non-UTF-8 key/value bytes answer None from the _str accessors while
    /// staying readable as raw bytes.
    #[test]
    fn log_append_time_kind_and_non_utf8_bytes() {
        let invalid = [0xFFu8, 0xFE, b'x'];
        let msg = Message::new(
            Some(&invalid),
            Some(&invalid),
            "t",
            0,
            0,
            Timestamp::LogAppendTime { millis: 42 },
            Headers::from_slice(&[]),
        );

        assert_eq!(
            msg.timestamp(),
            Timestamp::LogAppendTime { millis: 42 },
            "kind preserved, not collapsed to CreateTime"
        );
        assert_eq!(msg.timestamp().millis(), Some(42));

        assert_eq!(msg.key_str(), None, "non-UTF-8 key is None, not lossy");
        assert_eq!(msg.value_str(), None, "non-UTF-8 value is None, not lossy");
        assert_eq!(msg.key(), Some(&invalid[..]), "raw bytes still readable");
        assert_eq!(msg.value(), Some(&invalid[..]));
    }
}
