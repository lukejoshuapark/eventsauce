use crate::{format_event_sort_key, MapErrToStoreError};
use eventsauce::StoreError;

use aws_sdk_dynamodb::types::{AttributeValue, ConditionCheck, TransactWriteItem};
use uuid::Uuid;

pub fn create_condition_check_for_last_persisted_event(table_name: &str, stream_id: Uuid, last_persisted_event_sequence_number: u32) -> Result<TransactWriteItem, StoreError> {
    let last_persisted_event_check = ConditionCheck::builder()
        .table_name(table_name)
        .key("StreamId", AttributeValue::S(format!("STREAM#{}", stream_id)))
        .key("Name", AttributeValue::S(format_event_sort_key(last_persisted_event_sequence_number)))
        .condition_expression("attribute_exists(#s) AND attribute_exists(#n)")
        .expression_attribute_names("#s", "StreamId")
        .expression_attribute_names("#n", "Name")
        .build()
        .map_err_to_store_error()?;

    let transaction_item = TransactWriteItem::builder()
        .condition_check(last_persisted_event_check)
        .build();

    Ok(transaction_item)
}
