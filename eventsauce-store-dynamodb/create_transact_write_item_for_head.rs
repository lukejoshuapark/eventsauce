use crate::{HEAD_SORT_KEY, MapErrToStoreError};
use eventsauce::StoreError;

use aws_sdk_dynamodb::types::{AttributeValue, Put, ReturnValuesOnConditionCheckFailure, TransactWriteItem, Update};
use uuid::Uuid;

pub fn create_transact_write_item_for_head(table_name: &str, stream_id: Uuid, preceding_event_sequence_number: Option<u32>, version: u32) -> Result<TransactWriteItem, StoreError> {
    let Some(preceding_event_sequence_number) = preceding_event_sequence_number else {
        let put = Put::builder()
            .table_name(table_name)
            .item("StreamId", AttributeValue::S(format!("STREAM#{}", stream_id)))
            .item("Name", AttributeValue::S(HEAD_SORT_KEY.to_string()))
            .item("Version", AttributeValue::N(version.to_string()))
            .condition_expression("attribute_not_exists(#s) AND attribute_not_exists(#n)")
            .expression_attribute_names("#s", "StreamId")
            .expression_attribute_names("#n", "Name")
            .build()
            .map_err_to_store_error()?;

        return Ok(TransactWriteItem::builder().put(put).build());
    };

    let update = Update::builder()
        .table_name(table_name)
        .key("StreamId", AttributeValue::S(format!("STREAM#{}", stream_id)))
        .key("Name", AttributeValue::S(HEAD_SORT_KEY.to_string()))
        .update_expression("SET #v = :v")
        .condition_expression("#v = :p")
        .expression_attribute_names("#v", "Version")
        .expression_attribute_values(":v", AttributeValue::N(version.to_string()))
        .expression_attribute_values(":p", AttributeValue::N(preceding_event_sequence_number.to_string()))
        .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
        .build()
        .map_err_to_store_error()?;

    Ok(TransactWriteItem::builder().update(update).build())
}
