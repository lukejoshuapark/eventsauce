use crate::{Aggregate, State, StoreError};

use uuid::Uuid;

/// A backing store capable of persisting and retrieving aggregates of state
/// `S`.
pub trait Store<S : State> {
    /// Persists an aggregate's pending events.
    ///
    /// Implementations are expected to fail with [`StoreError::Contention`]
    /// if the stream has been concurrently modified since the aggregate was
    /// loaded.
    fn persist(&self, aggregate: Aggregate<S>) -> impl Future<Output = Result<(), StoreError>>;

    /// Retrieves the aggregate for the given stream, or `None` if no stream
    /// exists with that identifier.
    fn retrieve(&self, stream_id: Uuid) -> impl Future<Output = Result<Option<Aggregate<S>>, StoreError>>;
}
