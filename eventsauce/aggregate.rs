use crate::{AggregateError, Event, State};

use uuid::Uuid;

/// A wrapper around a [`State`] that tracks its version and any events
/// produced since it was last persisted.
///
/// An `Aggregate` is created fresh via [`from_creation`](Aggregate::from_creation),
/// reconstructed from a persisted stream via [`from_events`](Aggregate::from_events),
/// or rehydrated from a previously computed state via
/// [`from_state`](Aggregate::from_state). Once created, [`mutate`](Aggregate::mutate)
/// applies new events, and [`take`](Aggregate::take) hands ownership of the
/// state and pending events back out, typically so a [`Store`](crate::Store)
/// can persist them.
#[derive(Debug)]
pub struct Aggregate<S : State> {
    state: S,
    pending_events: Vec<S::Event>,
    version: u32
}

impl<S : State> Aggregate<S> {
    /// Creates a new aggregate from an unpersisted creation event.
    ///
    /// The event is assigned sequence number 0 and recorded as a pending
    /// event to be persisted.
    ///
    /// # Panics
    ///
    /// Panics if `creation_event` already has a sequence number, since that
    /// indicates it has already been persisted; use
    /// [`from_events`](Aggregate::from_events) to reconstruct an aggregate
    /// from a persisted stream instead.
    pub fn from_creation(mut creation_event: S::CreationEvent) -> Self {
        if creation_event.sequence_number().is_some() {
            panic!("This creation event has previously been persisted.  Use from_events for reconstructing an aggregate from persisted events.");
        }

        let state = S::new(&creation_event);
        creation_event.set_sequence_number(0);

        Aggregate {
            state,
            pending_events: vec![creation_event.into()],
            version: 0
        }
    }

    /// Rehydrates an aggregate from an already-computed state and version,
    /// with no pending events.
    ///
    /// This is typically used when a store persists a snapshot of the state
    /// alongside its version, avoiding the need to replay the full event
    /// stream.
    pub fn from_state(state: S, version: u32) -> Self {
        Aggregate {
            state,
            pending_events: Vec::new(),
            version
        }
    }

    /// Reconstructs an aggregate by replaying a persisted event stream from
    /// the beginning.
    ///
    /// The first event must be the stream's creation event with sequence
    /// number 0, and every subsequent event must carry the next consecutive
    /// sequence number.
    ///
    /// # Errors
    ///
    /// Returns [`AggregateError::NoEventsInStream`] if `events` is empty,
    /// [`AggregateError::State`] (wrapping
    /// [`StateError::FirstEventIsNotCreationEvent`](crate::StateError::FirstEventIsNotCreationEvent))
    /// if the first event is not a creation event,
    /// [`AggregateError::PersistedEventHasNoSequenceNumber`] if any event is
    /// missing its sequence number, or [`AggregateError::UnexpectedSequenceNumber`]
    /// if the sequence numbers are not consecutive starting from 0.
    pub fn from_events(events: Vec<S::Event>) -> Result<Self, AggregateError> {
        let mut event_iter = events.into_iter();
        let first_event = event_iter.next().ok_or(AggregateError::NoEventsInStream)?;
        let creation_event: S::CreationEvent = first_event.try_into()?;

        let Some(mut version) = creation_event.sequence_number() else {
            return Err(AggregateError::PersistedEventHasNoSequenceNumber);
        };

        if version != 0 {
            return Err(AggregateError::UnexpectedSequenceNumber { expected: 0, actual: version });
        }

        let mut state = S::new(&creation_event);
        for event in event_iter {
            let expected = version + 1;
            let Some(actual) = event.sequence_number() else {
                return Err(AggregateError::PersistedEventHasNoSequenceNumber);
            };

            if actual != expected {
                return Err(AggregateError::UnexpectedSequenceNumber { expected, actual });
            }

            state.apply(&event);
            version = actual;
        }

        let aggregate = Aggregate {
            state,
            pending_events: Vec::new(),
            version
        };

        Ok(aggregate)
    }

    /// Builds and applies a new event to this aggregate.
    ///
    /// `f` is given this aggregate's stream ID and must return the event to
    /// apply. The event is assigned the next sequence number, folded into
    /// the current state, and recorded as pending so it can later be
    /// persisted.
    pub fn mutate<T : Into<S::Event>, F : FnOnce(Uuid) -> T>(&mut self, f: F) {
        let mut event = f(self.state.stream_id()).into();
        let sequence_number = self.version + 1;
        event.set_sequence_number(sequence_number);

        self.state.apply(&event);
        self.pending_events.push(event);
        self.version = sequence_number;
    }

    /// The current derived state of this aggregate.
    pub fn state(&self) -> &S {
        &self.state
    }

    /// The sequence number of the most recent event applied to this
    /// aggregate.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Consumes this aggregate, returning its state, any events that have
    /// not yet been persisted, and its current version.
    pub fn take(self) -> (S, Vec<S::Event>, u32) {
        (self.state, self.pending_events, self.version)
    }
}
