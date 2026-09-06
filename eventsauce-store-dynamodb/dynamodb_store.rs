use crate::{create_condition_check_for_last_persisted_event, create_transact_write_item_for_event, create_transact_write_item_for_state, persist_state, retrieve_state, retrieve_stream_events, to_store_error_for_transaction};
use eventsauce::{Aggregate, Event, State, Store, StoreError};

use std::marker::PhantomData;

use aws_sdk_dynamodb::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

const MAX_TRANSACTION_ITEM_COUNT: usize = 100;

/// A [`Store`] that persists aggregates of state `S` to a DynamoDB table.
///
/// - [`persist`](Store::persist) writes the new state and events in a single
///   DynamoDB transaction, capped at DynamoDB's 100-item transaction limit:
///   up to 99 events for a new stream, or 98 if the stream already exists,
///   since persisting to an existing stream adds a condition check on the
///   preceding event as an extra transaction item.
///
/// - [`retrieve`](Store::retrieve) prefers reading the stored state directly
///   when its logical version matches `S`, falling back to rebuilding from
///   events otherwise. If rebuilt from events, the state is persisted back
///   for next time.
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
        if let Some((state, version)) = retrieve_state::<S>(&self.table_name, &self.client, stream_id).await? {
            let aggregate = Aggregate::from_state(state, version);
            return Ok(Some(aggregate));
        };

        let Some(events) = retrieve_stream_events::<S>(&self.table_name, &self.client, stream_id).await? else {
            return Ok(None);
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

        let max_event_count = match preceding_event_sequence_number {
            Some(_) => MAX_TRANSACTION_ITEM_COUNT - 2,
            None => MAX_TRANSACTION_ITEM_COUNT - 1
        };

        if events.len() > max_event_count {
            return Err(StoreError::TooManyEvents {
                stream_id,
                max_event_count,
                actual_event_count: events.len()
            });
        }

        let mut transaction_builder = self.client.transact_write_items();
        if let Some(preceding_event_sequence_number) = preceding_event_sequence_number {
            let last_persisted_event_check = create_condition_check_for_last_persisted_event(&self.table_name, stream_id, preceding_event_sequence_number)?;
            transaction_builder = transaction_builder.transact_items(last_persisted_event_check);
        }

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
}
