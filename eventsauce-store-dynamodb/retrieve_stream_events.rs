use crate::MapErrToStoreError;
use eventsauce::{Event, State, StoreError};

use aws_sdk_dynamodb::{types::AttributeValue, Client};
use serde::de::DeserializeOwned;
use uuid::Uuid;

pub async fn retrieve_stream_events<S>(table_name: &str, client: &Client, stream_id: Uuid) -> Result<Option<Vec<S::Event>>, StoreError>
where
    S : State,
    <S as State>::Event : Event + DeserializeOwned,
    <S as State>::CreationEvent : DeserializeOwned
{
    let mut next_sequence_number: u32 = 0;
    let mut events: Vec<S::Event> = vec![];
    let mut exclusive_start_key = None;

    loop {
        let output = client.query()
            .table_name(table_name)
            .consistent_read(true)
            .key_condition_expression("#s = :s AND begins_with(#n, :e)")
            .expression_attribute_names("#s", "StreamId")
            .expression_attribute_names("#n", "Name")
            .expression_attribute_values(":s", AttributeValue::S(format!("STREAM#{}", stream_id)))
            .expression_attribute_values(":e", AttributeValue::S("EVENT#".to_string()))
            .set_exclusive_start_key(exclusive_start_key)
            .send()
            .await
            .map_err_to_store_error()?;

        for item in output.items() {
            let event = event_from_item::<S::Event>(item)?;
            if event.stream_id() != stream_id {
                return Err(StoreError::InvalidData(format!("Event streamId '{}' does not match expected streamId '{}'", event.stream_id(), stream_id)));
            }

            let Some(event_sequence_number) = event.sequence_number() else {
                return Err(StoreError::InvalidEvent(String::from("Event is missing sequence number")));
            };

            if event_sequence_number != next_sequence_number {
                return Err(StoreError::InvalidEvent(format!("Event sequence number '{}' does not match expected sequence number '{}'", event_sequence_number, next_sequence_number)));
            }

            events.push(event);
            next_sequence_number += 1;
        }

        exclusive_start_key = output.last_evaluated_key().cloned();
        if exclusive_start_key.is_none() {
            break;
        }
    }

    if events.is_empty() {
        return Ok(None);
    }

    Ok(Some(events))
}

fn event_from_item<E : DeserializeOwned>(item: &std::collections::HashMap<String, AttributeValue>) -> Result<E, StoreError> {
    let Some(AttributeValue::S(serialized_event)) = item.get("Data") else {
        return Err(StoreError::InvalidData(String::from("DynamoDB item is missing 'Data'")));
    };

    let event: E = serde_json::from_str(serialized_event).map_err_to_store_error()?;
    Ok(event)
}
