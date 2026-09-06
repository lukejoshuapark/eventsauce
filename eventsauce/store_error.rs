use thiserror::Error;
use uuid::Uuid;

use crate::AggregateError;

/// Errors that can occur while persisting or retrieving an aggregate from a
/// [`Store`](crate::Store).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    /// Reconstructing the aggregate from its stream failed.
    #[error("Aggregate: {0}")]
    Aggregate(#[from] AggregateError),

    /// The stream was concurrently modified, so persisting these events
    /// would have violated stream integrity (e.g. duplicated a sequence
    /// number). The caller should typically retrieve the latest state and
    /// retry.
    #[error("Completing the persistence operation would have violated stream integrity.  The stream is likely being concurrently modified.")]
    Contention,

    /// Data retrieved from the store could not be interpreted, for a reason
    /// other than serialization or a specific creation/subsequent event
    /// being invalid.
    #[error("The data retrieved from the store was invalid: {0}")]
    InvalidData(String),

    /// The first event retrieved from the stream was not a valid creation
    /// event.
    #[error("The first event in the stream was invalid: {0}")]
    InvalidCreationEvent(String),

    /// An event retrieved from the stream (other than the creation event)
    /// was invalid.
    #[error("An event in the stream was invalid: {0}")]
    InvalidEvent(String),

    /// The events being persisted were expected to follow a specific event
    /// already in the stream, but that event was not found. This indicates
    /// the aggregate was built from data that is not (or is no longer) in
    /// the store.
    #[error("Stream '{stream_id}' does not contain the event with sequence number {sequence_number} that the persisted events were expected to follow.  The aggregate was built from data that is not in the store.")]
    MissingPrecedingEvent {
        /// The stream that was expected to contain the preceding event.
        stream_id: Uuid,
        /// The sequence number of the preceding event that could not be
        /// found.
        sequence_number: u32
    },

    /// The store's ratelimit was reached.
    #[error("Store ratelimit reached: {0}")]
    Ratelimit(String),

    /// Serializing or deserializing data for the store failed.
    #[error("Store serialization or deserialization failure: {0}")]
    Serialization(String),

    /// The operation against the store timed out.
    #[error("Store timed out: {0}")]
    Timeout(String),

    /// More events were provided for persistence than the store allows in a
    /// single operation for one stream.
    #[error("There were too many events to persist for stream '{stream_id}'.  The maximum is {max_event_count}, but {actual_event_count} were provided.")]
    TooManyEvents {
        /// The stream the events were being persisted to.
        stream_id: Uuid,
        /// The maximum number of events the store allows in a single
        /// operation.
        max_event_count: usize,
        /// The number of events that were actually provided.
        actual_event_count: usize
    },

    /// The underlying store could not be reached.
    #[error("The underlying store is unavailable: {0}")]
    Unavailable(String),

    /// An error occurred that does not fit any other variant.
    #[error("Unknown store error: {0}")]
    Other(String)
}
