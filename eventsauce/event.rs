use time::OffsetDateTime;
use uuid::Uuid;

/// A single fact that has happened (or is about to happen) to an aggregate.
///
/// Events are ordered within a stream by their [`sequence_number`], and a
/// stream is identified by its [`stream_id`]. An event is unpersisted until
/// it has a sequence number and an `occurred_at` timestamp assigned to it,
/// typically by a store at the moment it is persisted.
///
/// [`sequence_number`]: Event::sequence_number
/// [`stream_id`]: Event::stream_id
pub trait Event {
    /// The unique identifier of this event.
    fn id(&self) -> Uuid;

    /// The identifier of the stream (aggregate instance) this event belongs
    /// to.
    fn stream_id(&self) -> Uuid;

    /// The time at which this event was persisted, or `None` if it has not
    /// yet been persisted.
    fn occurred_at(&self) -> Option<OffsetDateTime>;

    /// Records the time at which this event was persisted.
    fn set_occurred_at(&mut self, timestamp: OffsetDateTime);

    /// This event's position within its stream, or `None` if it has not yet
    /// been assigned one.
    fn sequence_number(&self) -> Option<u32>;

    /// Assigns this event's position within its stream.
    fn set_sequence_number(&mut self, sequence_number: u32);
}
