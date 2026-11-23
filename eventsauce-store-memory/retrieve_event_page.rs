use crate::{format_event_key, format_event_key_prefix, to_store_error};
use eventsauce::{Event, EventPage, StoreError};

use std::collections::HashMap;

use serde::de::DeserializeOwned;
use uuid::Uuid;

pub fn retrieve_event_page<E : Event + DeserializeOwned>(data: &HashMap<String, String>, stream_id: Uuid, before: Option<u32>, limit: usize) -> Result<Option<EventPage<E>>, StoreError> {
    let prefix = format_event_key_prefix(stream_id);

    let mut matching_keys: Vec<&String> = data.keys()
        .filter(|key| key.starts_with(&prefix))
        .collect();

    if matching_keys.is_empty() {
        return Ok(None);
    }

    // Keys zero-pad the sequence number, so lexical order is sequence order.
    let upper_bound_key = before.map(|before| format_event_key(stream_id, before));
    matching_keys.retain(|key| upper_bound_key.as_ref().is_none_or(|upper_bound_key| *key < upper_bound_key));
    matching_keys.sort_unstable_by(|a, b| b.cmp(a));
    matching_keys.truncate(limit);

    let events = matching_keys.into_iter()
        .map(|key| serde_json::from_str(&data[key]).map_err(to_store_error))
        .collect::<Result<Vec<E>, StoreError>>()?;

    let next = match events.last() {
        Some(oldest_event) => {
            let Some(sequence_number) = oldest_event.sequence_number() else {
                return Err(StoreError::InvalidEvent(String::from("Event is missing sequence number")));
            };

            Some(sequence_number).filter(|sequence_number| *sequence_number > 0)
        },
        None => None
    };

    Ok(Some(EventPage { events, next }))
}
