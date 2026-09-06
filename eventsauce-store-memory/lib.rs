//! An in-memory [`Store`](eventsauce::Store) implementation for
//! [`eventsauce`](https://docs.rs/eventsauce), intended for tests and
//! examples.
//!
//! Events are serialized with `serde_json` into a single
//! `HashMap<String, String>`, keyed by stream ID and sequence number. State
//! is never persisted; every retrieve rebuilds the aggregate by replaying
//! its events.

mod format_event_key;
mod memory_store;
mod retrieve_stream_events;
mod to_store_error;

pub(crate) use format_event_key::*;
pub use memory_store::*;
pub(crate) use retrieve_stream_events::*;
pub(crate) use to_store_error::*;
