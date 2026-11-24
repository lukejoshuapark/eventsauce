use crate::{Aggregate, EventPage, State, StoreError};

use std::num::NonZeroU32;

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

    /// Retrieves a page of the given stream's events, newest first, or
    /// `None` if no stream exists with that identifier.
    ///
    /// Pass `None` as `before` for the newest events, then each page's
    /// [`next`](EventPage::next) to continue. A page holds `limit` events
    /// unless fewer remain.
    fn retrieve_events(&self, stream_id: Uuid, before: Option<u32>, limit: NonZeroU32) -> impl Future<Output = Result<Option<EventPage<S::Event>>, StoreError>>;
}
