use crate::{format_event_sort_key, MapErrToStoreError};
use eventsauce::{Event, StoreError};

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use serde::Serialize;

pub fn create_transact_write_item_for_event<E : Event + Serialize>(table_name: &str, event: E) -> Result<TransactWriteItem, StoreError> {
    let serialized_event = serde_json::to_string(&event).map_err_to_store_error()?;

    let Some(event_sequence_number) = event.sequence_number() else {
        return Err(StoreError::InvalidEvent(String::from("Event is missing sequence number")));
    };

    let put = Put::builder()
        .table_name(table_name)
        .item("StreamId", AttributeValue::S(format!("STREAM#{}", event.stream_id())))
        .item("Name", AttributeValue::S(format_event_sort_key(event_sequence_number)))
        .item("Data", AttributeValue::S(serialized_event))
        .condition_expression("attribute_not_exists(#s) AND attribute_not_exists(#n)")
        .expression_attribute_names("#s", "StreamId")
        .expression_attribute_names("#n", "Name")
        .build()
        .map_err_to_store_error()?;

    let transaction_item = TransactWriteItem::builder()
        .put(put)
        .build();

    Ok(transaction_item)
}
