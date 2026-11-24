use crate::{HEAD_SORT_KEY, MapErrToStoreError};
use eventsauce::StoreError;

use aws_sdk_dynamodb::types::{AttributeValue, ConditionCheck, TransactWriteItem};
use uuid::Uuid;

pub fn create_condition_check_for_head(table_name: &str, stream_id: Uuid, version: u32) -> Result<TransactWriteItem, StoreError> {
    let condition_check = ConditionCheck::builder()
        .table_name(table_name)
        .key("StreamId", AttributeValue::S(format!("STREAM#{}", stream_id)))
        .key("Name", AttributeValue::S(HEAD_SORT_KEY.to_string()))
        .condition_expression("#v = :v")
        .expression_attribute_names("#v", "Version")
        .expression_attribute_values(":v", AttributeValue::N(version.to_string()))
        .build()
        .map_err_to_store_error()?;

    Ok(TransactWriteItem::builder().condition_check(condition_check).build())
}
