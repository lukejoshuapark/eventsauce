use crate::MapErrToStoreError;
use eventsauce::{State, StoreError};

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use serde::Serialize;

pub fn create_transact_write_item_for_state<S : Serialize + State>(table_name: &str, state: S, version: u32) -> Result<TransactWriteItem, StoreError> {
    let serialized_state = serde_json::to_string(&state).map_err_to_store_error()?;

    let put = Put::builder()
        .table_name(table_name)
        .item("StreamId", AttributeValue::S(format!("STREAM#{}", state.stream_id())))
        .item("Name", AttributeValue::S(format!("STATE#{}", S::logical_version())))
        .item("Version", AttributeValue::N(version.to_string()))
        .item("Data", AttributeValue::S(serialized_state))
        .build()
        .map_err_to_store_error()?;

    let transaction_item = TransactWriteItem::builder()
        .put(put)
        .build();

    Ok(transaction_item)
}
