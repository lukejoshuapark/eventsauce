use crate::ToStoreError;
use eventsauce::StoreError;

use aws_sdk_dynamodb::error::SdkError as DynamoSdkError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{AttributeValue, CancellationReason};
use uuid::Uuid;

pub fn to_store_error_for_transaction(
    error: &DynamoSdkError<TransactWriteItemsError>,
    stream_id: Uuid,
    preceding_event_sequence_number: Option<u32>
) -> StoreError {
    let DynamoSdkError::ServiceError(service_error) = error else {
        return error.to_store_error();
    };

    let TransactWriteItemsError::TransactionCanceledException(cancellation) = service_error.err() else {
        return error.to_store_error();
    };

    for (index, reason) in cancellation.cancellation_reasons().iter().enumerate() {
        if reason.code() != Some("ConditionalCheckFailed") {
            continue;
        }

        let Some(sequence_number) = preceding_event_sequence_number else {
            return StoreError::Contention;
        };

        if index != 0 {
            return StoreError::Contention;
        }

        return match head_version_from_reason(reason) {
            Some(head_version) if head_version > sequence_number => StoreError::Contention,
            _ => StoreError::MissingPrecedingEvent { stream_id, sequence_number }
        };
    }

    if cancellation.cancellation_reasons().iter().any(|reason| reason.code() == Some("TransactionConflict")) {
        return StoreError::Contention;
    }

    error.to_store_error()
}

fn head_version_from_reason(reason: &CancellationReason) -> Option<u32> {
    let Some(AttributeValue::N(version_str)) = reason.item()?.get("Version") else {
        return None;
    };

    version_str.parse().ok()
}
