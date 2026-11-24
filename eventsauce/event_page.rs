/// A page of a stream's events, as returned by
/// [`Store::retrieve_events`](crate::Store::retrieve_events).
#[derive(Debug, Clone)]
pub struct EventPage<E> {
    /// The events in this page, newest first.
    pub events: Vec<E>,

    /// The `before` cursor for the next (older) page, or `None` if there
    /// are no older events.
    pub next: Option<u32>
}
