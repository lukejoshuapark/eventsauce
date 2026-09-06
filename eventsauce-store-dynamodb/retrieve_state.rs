use crate::MapErrToStoreError;
use eventsauce::{State, StoreError};

use aws_sdk_dynamodb::{types::AttributeValue, Client};
use serde::de::DeserializeOwned;
use uuid::Uuid;

pub async fn retrieve_state<S : DeserializeOwned + State>(table_name: &str, client: &Client, stream_id: Uuid) -> Result<Option<(S, u32)>, StoreError> {
    let logical_version = S::logical_version();
    let output = client.get_item()
        .table_name(table_name)
        .key("StreamId", AttributeValue::S(format!("STREAM#{}", stream_id)))
        .key("Name", AttributeValue::S(format!("STATE#{}", logical_version)))
        .send()
        .await
        .map_err_to_store_error()?;

    let Some(item) = output.item() else {
        return Ok(None);
    };

    let Some(AttributeValue::N(version_str)) = item.get("Version") else {
        return Err(StoreError::InvalidData(format!("DynamoDB item for StreamId '{}' is missing 'Version'", stream_id)));
    };

    let version: u32 = version_str.parse().map_err_to_store_error()?;

    let Some(AttributeValue::S(serialized_state)) = item.get("Data") else {
        return Err(StoreError::InvalidData(format!("DynamoDB item for StreamId '{}' is missing 'Data'", stream_id)));
    };

    let state: S = serde_json::from_str(serialized_state).map_err_to_store_error()?;
    Ok(Some((state, version)))
}
