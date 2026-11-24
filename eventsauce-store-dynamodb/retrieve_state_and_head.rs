use crate::{HEAD_SORT_KEY, MapErrToStoreError};
use eventsauce::{State, StoreError};

use std::collections::HashMap;

use aws_sdk_dynamodb::{types::{AttributeValue, KeysAndAttributes}, Client};
use serde::de::DeserializeOwned;
use uuid::Uuid;

pub type StoredState<S> = Option<(S, u32)>;

pub async fn retrieve_state_and_head<S : DeserializeOwned + State>(table_name: &str, client: &Client, stream_id: Uuid) -> Result<(StoredState<S>, Option<u32>), StoreError> {
    let partition_key = AttributeValue::S(format!("STREAM#{}", stream_id));
    let state_sort_key = format!("STATE#{}", S::logical_version());

    let keys_and_attributes = KeysAndAttributes::builder()
        .keys(HashMap::from([
            (String::from("StreamId"), partition_key.clone()),
            (String::from("Name"), AttributeValue::S(state_sort_key.clone()))
        ]))
        .keys(HashMap::from([
            (String::from("StreamId"), partition_key),
            (String::from("Name"), AttributeValue::S(HEAD_SORT_KEY.to_string()))
        ]))
        .consistent_read(true)
        .build()
        .map_err_to_store_error()?;

    let output = client.batch_get_item()
        .request_items(table_name, keys_and_attributes)
        .send()
        .await
        .map_err_to_store_error()?;

    if output.unprocessed_keys().is_some_and(|unprocessed_keys| !unprocessed_keys.is_empty()) {
        return Err(StoreError::Ratelimit(format!("DynamoDB did not process every key when retrieving StreamId '{}'", stream_id)));
    }

    let mut state = None;
    let mut head_version = None;

    for item in output.responses().and_then(|responses| responses.get(table_name)).into_iter().flatten() {
        let Some(AttributeValue::S(name)) = item.get("Name") else {
            return Err(StoreError::InvalidData(format!("DynamoDB item for StreamId '{}' is missing 'Name'", stream_id)));
        };

        let version = version_from_item(item, stream_id)?;

        if name == HEAD_SORT_KEY {
            head_version = Some(version);
            continue;
        }

        let Some(AttributeValue::S(serialized_state)) = item.get("Data") else {
            return Err(StoreError::InvalidData(format!("DynamoDB item for StreamId '{}' is missing 'Data'", stream_id)));
        };

        let deserialized_state: S = serde_json::from_str(serialized_state).map_err_to_store_error()?;
        state = Some((deserialized_state, version));
    }

    Ok((state, head_version))
}

fn version_from_item(item: &HashMap<String, AttributeValue>, stream_id: Uuid) -> Result<u32, StoreError> {
    let Some(AttributeValue::N(version_str)) = item.get("Version") else {
        return Err(StoreError::InvalidData(format!("DynamoDB item for StreamId '{}' is missing 'Version'", stream_id)));
    };

    version_str.parse().map_err_to_store_error()
}
