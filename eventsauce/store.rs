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
    /// The page holds events with sequence numbers strictly below `before`,
    /// or the stream's newest events when `before` is `None`. To page
    /// through the whole stream, start with `None` and pass each page's
    /// [`next`](EventPage::next) as `before` until it is `None`.
    ///
    /// A page holds exactly `limit` events, unless fewer than `limit` events
    /// remain below `before`, in which case it holds all of them. A page for
    /// an existing stream with no events below `before` (e.g. `before` is
    /// `Some(0)`) is empty, with `next` set to `None`.
    fn retrieve_events(&self, stream_id: Uuid, before: Option<u32>, limit: NonZeroU32) -> impl Future<Output = Result<Option<EventPage<S::Event>>, StoreError>>;
}
