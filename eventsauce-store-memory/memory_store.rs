use crate::{format_event_key, retrieve_stream_events, to_store_error};
use eventsauce::{Aggregate, Event, State, Store, StoreError};

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Mutex;

use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

/// A [`Store`] that keeps every event in an in-memory `HashMap<String,
/// String>`, serialized with `serde_json`.
///
/// State is never persisted, so [`retrieve`](Store::retrieve) always
/// rebuilds the aggregate by replaying its events. [`persist`](Store::persist)
/// appends the new events under a lock, failing with
/// [`StoreError::Contention`](eventsauce::StoreError::Contention) if any of
/// their sequence numbers have already been taken, or
/// [`StoreError::MissingPrecedingEvent`](eventsauce::StoreError::MissingPrecedingEvent)
/// if the event they were meant to follow is not present.
///
/// Intended for tests and examples; nothing is persisted beyond the
/// lifetime of the store.
pub struct MemoryStore<S : State> {
    _marker: PhantomData<S>,
    data: Mutex<HashMap<String, String>>
}

impl<S : State> MemoryStore<S> {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
            data: Mutex::new(HashMap::new())
        }
    }
}

impl<S : State> Default for MemoryStore<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Store<S> for MemoryStore<S>
where
    S : DeserializeOwned + Serialize + State,
    <S as State>::Event : DeserializeOwned + Serialize
{
    async fn retrieve(&self, stream_id: Uuid) -> Result<Option<Aggregate<S>>, StoreError> {
        let data = self.data.lock().unwrap();

        let Some(events) = retrieve_stream_events::<S::Event>(&data, stream_id)? else {
            return Ok(None);
        };

        let aggregate = Aggregate::from_events(events)?;
        Ok(Some(aggregate))
    }

    async fn persist(&self, aggregate: Aggregate<S>) -> Result<(), StoreError> {
        let (state, events, version) = aggregate.take();
        if events.is_empty() {
            return Ok(());
        }

        let stream_id = state.stream_id();

        let Some(first_sequence_number) = (u64::from(version) + 1).checked_sub(events.len() as u64) else {
            return Err(StoreError::InvalidData(format!("StreamId '{}' has {} pending events, which is more than its version of {} can account for", stream_id, events.len(), version)));
        };

        let first_sequence_number = first_sequence_number as u32;
        let mut data = self.data.lock().unwrap();

        if let Some(preceding_sequence_number) = first_sequence_number.checked_sub(1) {
            let preceding_key = format_event_key(stream_id, preceding_sequence_number);
            if !data.contains_key(&preceding_key) {
                return Err(StoreError::MissingPrecedingEvent { stream_id, sequence_number: preceding_sequence_number });
            }
        }

        let now = time::OffsetDateTime::now_utc();
        let mut serialized_events = Vec::with_capacity(events.len());

        for (next_sequence_number, mut event) in (first_sequence_number..).zip(events) {
            if let Some(assigned_sequence_number) = event.sequence_number() && assigned_sequence_number != next_sequence_number {
                return Err(StoreError::InvalidEvent(format!("Event in stream '{}' has sequence number {}, but {} was expected", stream_id, assigned_sequence_number, next_sequence_number)));
            }

            if event.sequence_number().is_none() {
                return Err(StoreError::InvalidEvent(format!("Event in stream '{}' with an expected sequence number of {} is missing its sequence number", stream_id, next_sequence_number)));
            }

            let key = format_event_key(stream_id, next_sequence_number);
            if data.contains_key(&key) {
                return Err(StoreError::Contention);
            }

            event.set_occurred_at(now);
            event.set_sequence_number(next_sequence_number);

            let serialized_event = serde_json::to_string(&event).map_err(to_store_error)?;
            serialized_events.push((key, serialized_event));
        }

        for (key, serialized_event) in serialized_events {
            data.insert(key, serialized_event);
        }

        Ok(())
    }
}

/// Lets a borrowed store be used wherever an owned [`Store`] is expected
/// (e.g. [`concurrency_safe_persist`](eventsauce::concurrency_safe_persist)),
/// since [`MemoryStore`] can't cheaply be cloned into a new handle onto the
/// same data the way a networked store's client can.
impl<S> Store<S> for &MemoryStore<S>
where
    S : DeserializeOwned + Serialize + State,
    <S as State>::Event : DeserializeOwned + Serialize
{
    async fn retrieve(&self, stream_id: Uuid) -> Result<Option<Aggregate<S>>, StoreError> {
        (**self).retrieve(stream_id).await
    }

    async fn persist(&self, aggregate: Aggregate<S>) -> Result<(), StoreError> {
        (**self).persist(aggregate).await
    }
}
