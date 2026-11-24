use crate::{format_event_sort_key, HEAD_SORT_KEY, MapErrToStoreError};
use eventsauce::{Event, EventPage, State, StoreError};

use std::collections::HashMap;
use std::num::NonZeroU32;

use aws_sdk_dynamodb::{types::AttributeValue, Client};
use serde::de::DeserializeOwned;
use uuid::Uuid;

pub async fn retrieve_event_page<S>(table_name: &str, client: &Client, stream_id: Uuid, before: Option<u32>, limit: NonZeroU32) -> Result<Option<EventPage<S::Event>>, StoreError>
where
    S : State,
    <S as State>::Event : Event + DeserializeOwned
{
    let newest_sequence_number = match before {
        Some(before) => before.checked_sub(1),
        None => Some(u32::MAX)
    };

    let limit = usize::try_from(limit.get()).unwrap_or(usize::MAX);
    let mut events: Vec<S::Event> = vec![];

    if let Some(newest_sequence_number) = newest_sequence_number {
        let mut exclusive_start_key = None;

        loop {
            let remaining = i32::try_from(limit - events.len()).unwrap_or(i32::MAX);

            let output = client.query()
                .table_name(table_name)
                .consistent_read(true)
                .key_condition_expression("#s = :s AND #n BETWEEN :lo AND :hi")
                .expression_attribute_names("#s", "StreamId")
                .expression_attribute_names("#n", "Name")
                .expression_attribute_values(":s", AttributeValue::S(format!("STREAM#{}", stream_id)))
                .expression_attribute_values(":lo", AttributeValue::S(format_event_sort_key(0)))
                .expression_attribute_values(":hi", AttributeValue::S(format_event_sort_key(newest_sequence_number)))
                .scan_index_forward(false)
                .limit(remaining)
                .set_exclusive_start_key(exclusive_start_key)
                .send()
                .await
                .map_err_to_store_error()?;

            for item in output.items() {
                events.push(event_from_item::<S::Event>(item)?);
            }

            exclusive_start_key = output.last_evaluated_key().cloned();
            if events.len() >= limit || exclusive_start_key.is_none() {
                break;
            }
        }
    }

    if events.is_empty() {
        if !stream_exists(table_name, client, stream_id).await? {
            return Ok(None);
        }

        return Ok(Some(EventPage { events, next: None }));
    }

    let mut expected_sequence_number = None;
    let mut oldest_sequence_number = 0;

    for event in &events {
        if event.stream_id() != stream_id {
            return Err(StoreError::InvalidData(format!("Event streamId '{}' does not match expected streamId '{}'", event.stream_id(), stream_id)));
        }

        let Some(event_sequence_number) = event.sequence_number() else {
            return Err(StoreError::InvalidEvent(String::from("Event is missing sequence number")));
        };

        if let Some(expected_sequence_number) = expected_sequence_number && event_sequence_number != expected_sequence_number {
            return Err(StoreError::InvalidEvent(format!("Event sequence number '{}' does not match expected sequence number '{}'", event_sequence_number, expected_sequence_number)));
        }

        expected_sequence_number = event_sequence_number.checked_sub(1);
        oldest_sequence_number = event_sequence_number;
    }

    let next = Some(oldest_sequence_number).filter(|sequence_number| *sequence_number > 0);
    Ok(Some(EventPage { events, next }))
}

async fn stream_exists(table_name: &str, client: &Client, stream_id: Uuid) -> Result<bool, StoreError> {
    let output = client.get_item()
        .table_name(table_name)
        .consistent_read(true)
        .key("StreamId", AttributeValue::S(format!("STREAM#{}", stream_id)))
        .key("Name", AttributeValue::S(HEAD_SORT_KEY.to_string()))
        .projection_expression("#s")
        .expression_attribute_names("#s", "StreamId")
        .send()
        .await
        .map_err_to_store_error()?;

    Ok(output.item().is_some())
}

fn event_from_item<E : DeserializeOwned>(item: &HashMap<String, AttributeValue>) -> Result<E, StoreError> {
    let Some(AttributeValue::S(serialized_event)) = item.get("Data") else {
        return Err(StoreError::InvalidData(String::from("DynamoDB item is missing 'Data'")));
    };

    serde_json::from_str(serialized_event).map_err_to_store_error()
}
