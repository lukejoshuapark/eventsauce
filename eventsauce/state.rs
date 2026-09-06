use crate::{Event, StateError};

use uuid::Uuid;

/// The current, derived state of an aggregate, built up by replaying its
/// events.
///
/// A `State` is created from its [`CreationEvent`](State::CreationEvent) and
/// then folded over subsequent [`Event`](State::Event)s via [`apply`](State::apply),
/// yielding the current snapshot of an aggregate at some version. This is
/// the trait aggregate authors implement; [`Aggregate`](crate::Aggregate) is
/// the generic wrapper that drives it.
pub trait State {
    /// The event type that this state is built from, encompassing both the
    /// creation event and all subsequent events in the stream.
    type Event : Event + From<Self::CreationEvent>;

    /// The event type that initializes this state. Must be convertible from
    /// `Self::Event`, failing with [`StateError::FirstEventIsNotCreationEvent`]
    /// when the event is not actually a creation event.
    type CreationEvent : TryFrom<Self::Event, Error = StateError> + Event;

    /// Builds the initial state from the stream's creation event.
    fn new(event: &Self::CreationEvent) -> Self;

    /// The current schema version of this state, used to detect when
    /// persisted state was built from an older version of the aggregate's
    /// logic and needs to be rebuilt from its events.
    fn logical_version() -> u32;

    /// The identifier of the stream (aggregate instance) that this state
    /// belongs to.
    fn stream_id(&self) -> Uuid;

    /// Folds a single event into this state, mutating it in place.
    fn apply(&mut self, event: &Self::Event);
}
