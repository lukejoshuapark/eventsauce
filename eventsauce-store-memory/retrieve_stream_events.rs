use crate::{format_event_key_prefix, to_store_error};
use eventsauce::StoreError;

use std::collections::HashMap;

use serde::de::DeserializeOwned;
use uuid::Uuid;

pub fn retrieve_stream_events<E : DeserializeOwned>(data: &HashMap<String, String>, stream_id: Uuid) -> Result<Option<Vec<E>>, StoreError> {
    let prefix = format_event_key_prefix(stream_id);

    let mut matching_keys: Vec<&String> = data.keys()
        .filter(|key| key.starts_with(&prefix))
        .collect();

    if matching_keys.is_empty() {
        return Ok(None);
    }

    matching_keys.sort();

    let events = matching_keys.into_iter()
        .map(|key| serde_json::from_str(&data[key]).map_err(to_store_error))
        .collect::<Result<Vec<E>, StoreError>>()?;

    Ok(Some(events))
}
