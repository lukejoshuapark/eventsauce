use thiserror::Error;

/// Errors that can occur while converting an [`Event`](crate::Event) into a
/// [`State`](crate::State)'s [`CreationEvent`](crate::State::CreationEvent).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StateError {
    /// The event being converted was not the aggregate's creation event.
    #[error("The first event in the stream was not the creation event")]
    FirstEventIsNotCreationEvent
}
