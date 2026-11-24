use crate::{create_condition_check_for_head, to_store_error_for_transaction, MapErrToStoreError};
use eventsauce::{State, StoreError};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use serde::Serialize;

pub async fn persist_state<S : Serialize + State>(table_name: &str, client: &Client, state: &S, version: u32) -> Result<(), StoreError> {
    let stream_id = state.stream_id();
    let serialized_state = serde_json::to_string(state).map_err_to_store_error()?;

    let put = Put::builder()
        .table_name(table_name)
        .item("StreamId", AttributeValue::S(format!("STREAM#{}", stream_id)))
        .item("Name", AttributeValue::S(format!("STATE#{}", S::logical_version())))
        .item("Version", AttributeValue::N(version.to_string()))
        .item("Data", AttributeValue::S(serialized_state))
        .condition_expression("attribute_not_exists(#s) AND attribute_not_exists(#n)")
        .expression_attribute_names("#s", "StreamId")
        .expression_attribute_names("#n", "Name")
        .build()
        .map_err_to_store_error()?;

    let result = client.transact_write_items()
        .transact_items(TransactWriteItem::builder().put(put).build())
        .transact_items(create_condition_check_for_head(table_name, stream_id, version)?)
        .send()
        .await;

    if let Err(e) = result {
        return Err(to_store_error_for_transaction(&e, stream_id, None));
    }

    Ok(())
}
