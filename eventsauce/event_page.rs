/// A page of a stream's events, ordered newest first, as returned by
/// [`Store::retrieve_events`](crate::Store::retrieve_events).
#[derive(Debug, Clone)]
pub struct EventPage<E> {
    /// The events in this page, in descending sequence number order. Holds
    /// as many events as were requested, unless fewer remained in the
    /// stream.
    pub events: Vec<E>,

    /// The cursor to pass as `before` to fetch the next (older) page, or
    /// `None` once the stream's creation event has been returned.
    pub next: Option<u32>
}
