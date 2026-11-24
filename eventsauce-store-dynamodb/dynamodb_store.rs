use crate::{create_transact_write_item_for_event, create_transact_write_item_for_head, create_transact_write_item_for_state, persist_state, retrieve_event_page, retrieve_state_and_head, retrieve_stream_events, to_store_error_for_transaction};
use eventsauce::{Aggregate, Event, EventPage, State, Store, StoreError};

use std::marker::PhantomData;
use std::num::NonZeroU32;

use aws_sdk_dynamodb::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

const MAX_TRANSACTION_ITEM_COUNT: usize = 100;

const MAX_EVENT_COUNT: usize = MAX_TRANSACTION_ITEM_COUNT - 2;

/// A [`Store`] that persists aggregates of state `S` to a DynamoDB table.
///
/// - [`persist`](Store::persist) writes the events, state and stream head in
///   a single transaction, so at most 98 events can be persisted at once.
/// - [`retrieve`](Store::retrieve) reads the stored state and applies any
///   newer events, or rebuilds from events if there is no stored state.
/// - [`retrieve_events`](Store::retrieve_events) reads pages with a
///   descending `Query`, so its cost depends on the page size.
pub struct DynamoDbStore<S : State> {
    _marker: PhantomData<S>,
    table_name: String,
    client: Client
}

impl<S : State> DynamoDbStore<S> {
    /// Creates a store that reads and writes items in `table_name` using `client`.
    pub fn new(table_name: String, client: Client) -> Self {
        Self {
            _marker: PhantomData,
            table_name,
            client
        }
    }
}

impl<S> Store<S> for DynamoDbStore<S>
where
    S : DeserializeOwned + Serialize + State,
    <S as State>::Event : DeserializeOwned + Serialize,
    <S as State>::CreationEvent : DeserializeOwned
{
    async fn retrieve(&self, stream_id: Uuid) -> Result<Option<Aggregate<S>>, StoreError> {
        let (stored_state, head_version) = retrieve_state_and_head::<S>(&self.table_name, &self.client, stream_id).await?;

        let Some(head_version) = head_version else {
            if stored_state.is_some() {
                return Err(StoreError::InvalidData(format!("StreamId '{}' has stored state but no head", stream_id)));
            }

            return Ok(None);
        };

        if let Some((mut state, mut version)) = stored_state {
            if version < head_version {
                let Some(newer_events) = retrieve_stream_events::<S>(&self.table_name, &self.client, stream_id, version + 1).await? else {
                    return Err(StoreError::InvalidData(format!("StreamId '{}' has a head at version {}, but no events after its stored state at version {}", stream_id, head_version, version)));
                };

                for event in &newer_events {
                    state.apply(event);
                }

                version += newer_events.len() as u32;
            }

            let aggregate = Aggregate::from_state(state, version);
            return Ok(Some(aggregate));
        };

        let Some(events) = retrieve_stream_events::<S>(&self.table_name, &self.client, stream_id, 0).await? else {
            return Err(StoreError::InvalidData(format!("StreamId '{}' has a head at version {}, but no events", stream_id, head_version)));
        };

        let aggregate = Aggregate::from_events(events)?;

        if let Err(e) = persist_state(&self.table_name, &self.client, aggregate.state(), aggregate.version()).await {
            let StoreError::Contention = e else {
                return Err(e);
            };
        }

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
        let preceding_event_sequence_number = first_sequence_number.checked_sub(1);

        if events.len() > MAX_EVENT_COUNT {
            return Err(StoreError::TooManyEvents {
                stream_id,
                max_event_count: MAX_EVENT_COUNT,
                actual_event_count: events.len()
            });
        }

        let head_transaction_item = create_transact_write_item_for_head(&self.table_name, stream_id, preceding_event_sequence_number, version)?;
        let mut transaction_builder = self.client.transact_write_items().transact_items(head_transaction_item);

        let state_transaction_item = create_transact_write_item_for_state(&self.table_name, state, version)?;
        transaction_builder = transaction_builder.transact_items(state_transaction_item);

        let now = time::OffsetDateTime::now_utc();

        for (next_sequence_number, mut event) in (first_sequence_number..).zip(events) {
            if let Some(assigned_sequence_number) = event.sequence_number() && assigned_sequence_number != next_sequence_number {
                return Err(StoreError::InvalidEvent(format!("Event in stream '{}' has sequence number {}, but {} was expected", stream_id, assigned_sequence_number, next_sequence_number)));
            }

            if event.sequence_number().is_none() {
                return Err(StoreError::InvalidEvent(format!("Event in stream '{}' with an expected sequence number of {} is missing its sequence number", stream_id, next_sequence_number)));
            }

            event.set_occurred_at(now);
            event.set_sequence_number(next_sequence_number);

            let event_transaction_item = create_transact_write_item_for_event(&self.table_name, event)?;
            transaction_builder = transaction_builder.transact_items(event_transaction_item);
        }

        if let Err(e) = transaction_builder.send().await {
            return Err(to_store_error_for_transaction(&e, stream_id, preceding_event_sequence_number));
        }

        Ok(())
    }

    async fn retrieve_events(&self, stream_id: Uuid, before: Option<u32>, limit: NonZeroU32) -> Result<Option<EventPage<S::Event>>, StoreError> {
        retrieve_event_page::<S>(&self.table_name, &self.client, stream_id, before, limit).await
    }
}
