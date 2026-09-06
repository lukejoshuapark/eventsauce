use crate::StateError;

use thiserror::Error;

/// Errors that can occur while rebuilding or persisting an aggregate from its
/// event stream.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AggregateError {
    /// The stream's events could not be converted into the aggregate's
    /// state.
    #[error("State: {0}")]
    State(#[from] StateError),

    /// The stream contained no events at all, so the aggregate cannot be
    /// initialized.
    #[error("The stream has no events")]
    NoEventsInStream,

    /// A persisted event was missing its sequence number, so its position in
    /// the stream cannot be determined.
    #[error("A persisted event has no sequence number")]
    PersistedEventHasNoSequenceNumber,

    /// An event was replayed out of order: the sequence number it carried did
    /// not match the sequence number expected next in the stream.
    #[error("Expected an event with sequence number {expected}, but found {actual}")]
    UnexpectedSequenceNumber {
        /// The sequence number that was expected next.
        expected: u32,
        /// The sequence number actually found on the event.
        actual: u32,
    }
}
