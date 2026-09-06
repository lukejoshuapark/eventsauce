use crate::MapErrToStoreError;
use eventsauce::{State, StoreError};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use serde::Serialize;

pub async fn persist_state<S : Serialize + State>(table_name: &str, client: &Client, state: &S, version: u32) -> Result<(), StoreError> {
    let serialized_state = serde_json::to_string(state).map_err_to_store_error()?;

    client.put_item()
        .table_name(table_name)
        .item("StreamId", AttributeValue::S(format!("STREAM#{}", state.stream_id())))
        .item("Name", AttributeValue::S(format!("STATE#{}", S::logical_version())))
        .item("Version", AttributeValue::N(version.to_string()))
        .item("Data", AttributeValue::S(serialized_state))
        .condition_expression("attribute_not_exists(#s) AND attribute_not_exists(#n)")
        .expression_attribute_names("#s", "StreamId")
        .expression_attribute_names("#n", "Name")
        .send()
        .await
        .map_err_to_store_error()?;

    Ok(())
}
