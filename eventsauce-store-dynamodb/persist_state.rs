use crate::{format_event_sort_key, to_store_error_for_transaction, MapErrToStoreError};
use eventsauce::{State, StoreError};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{AttributeValue, ConditionCheck, Put, TransactWriteItem};
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

    let mut transaction_builder = client.transact_write_items()
        .transact_items(TransactWriteItem::builder().put(put).build());

    if let Some(next_sequence_number) = version.checked_add(1) {
        let next_event_check = ConditionCheck::builder()
            .table_name(table_name)
            .key("StreamId", AttributeValue::S(format!("STREAM#{}", stream_id)))
            .key("Name", AttributeValue::S(format_event_sort_key(next_sequence_number)))
            .condition_expression("attribute_not_exists(#s) AND attribute_not_exists(#n)")
            .expression_attribute_names("#s", "StreamId")
            .expression_attribute_names("#n", "Name")
            .build()
            .map_err_to_store_error()?;

        transaction_builder = transaction_builder.transact_items(TransactWriteItem::builder().condition_check(next_event_check).build());
    }

    if let Err(e) = transaction_builder.send().await {
        return Err(to_store_error_for_transaction(&e, stream_id, None));
    }

    Ok(())
}
