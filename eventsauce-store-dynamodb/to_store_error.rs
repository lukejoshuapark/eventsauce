use eventsauce::StoreError;

use std::error::Error;
use std::num::ParseIntError;

use aws_sdk_dynamodb::error::SdkError as DynamoSdkError;
use aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemError;
use aws_sdk_dynamodb::operation::get_item::GetItemError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;

pub trait ToStoreError : Error {
    fn to_store_error(&self) -> StoreError;
}

pub trait MapErrToStoreError<T> {
    fn map_err_to_store_error(self) -> Result<T, StoreError>;
}

impl<T, E : ToStoreError> MapErrToStoreError<T> for Result<T, E> {
    fn map_err_to_store_error(self) -> Result<T, StoreError> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(e.to_store_error())
        }
    }
}

impl ToStoreError for aws_sdk_dynamodb::error::BuildError {
    fn to_store_error(&self) -> StoreError {
        StoreError::Other(self.to_string())
    }
}

impl ToStoreError for serde_json::Error {
    fn to_store_error(&self) -> StoreError {
        StoreError::Serialization(self.to_string())
    }
}

impl ToStoreError for ParseIntError {
    fn to_store_error(&self) -> StoreError {
        StoreError::InvalidData(format!("Next Sequence Number is not a valid u32: {}", self))
    }
}

impl<E : 'static + ToStoreError> ToStoreError for DynamoSdkError<E> {
    fn to_store_error(&self) -> StoreError {
        match self {
            DynamoSdkError::TimeoutError(_) => StoreError::Timeout(self.to_string()),
            DynamoSdkError::DispatchFailure(_) => StoreError::Unavailable(self.to_string()),
            DynamoSdkError::ResponseError(_) => StoreError::Unavailable(self.to_string()),
            DynamoSdkError::ServiceError(e) => e.err().to_store_error(),
            _ => StoreError::Other(self.to_string())
        }
    }
}

impl ToStoreError for BatchGetItemError {
    fn to_store_error(&self) -> StoreError {
        match self {
            BatchGetItemError::InternalServerError(_) => StoreError::Unavailable(self.to_string()),
            BatchGetItemError::InvalidEndpointException(_) => StoreError::Other(self.to_string()),
            BatchGetItemError::ProvisionedThroughputExceededException(_) => StoreError::Ratelimit(self.to_string()),
            BatchGetItemError::RequestLimitExceeded(_) => StoreError::Ratelimit(self.to_string()),
            BatchGetItemError::ResourceNotFoundException(_) => StoreError::Unavailable(self.to_string()),
            BatchGetItemError::ThrottlingException(_) => StoreError::Ratelimit(self.to_string()),
            _ => StoreError::Other(self.to_string())
        }
    }
}

impl ToStoreError for GetItemError {
    fn to_store_error(&self) -> StoreError {
        match self {
            GetItemError::InternalServerError(_) => StoreError::Unavailable(self.to_string()),
            GetItemError::InvalidEndpointException(_) => StoreError::Other(self.to_string()),
            GetItemError::ProvisionedThroughputExceededException(_) => StoreError::Ratelimit(self.to_string()),
            GetItemError::ResourceNotFoundException(_) => StoreError::Unavailable(self.to_string()),
            GetItemError::ThrottlingException(_) => StoreError::Ratelimit(self.to_string()),
            _ => StoreError::Other(self.to_string())
        }
    }
}

impl ToStoreError for PutItemError {
    fn to_store_error(&self) -> StoreError {
        match self {
            PutItemError::ConditionalCheckFailedException(_) => StoreError::Contention,
            PutItemError::InternalServerError(_) => StoreError::Unavailable(self.to_string()),
            PutItemError::InvalidEndpointException(_) => StoreError::Other(self.to_string()),
            PutItemError::ProvisionedThroughputExceededException(_) => StoreError::Ratelimit(self.to_string()),
            PutItemError::ResourceNotFoundException(_) => StoreError::Unavailable(self.to_string()),
            PutItemError::ThrottlingException(_) => StoreError::Ratelimit(self.to_string()),
            PutItemError::TransactionConflictException(_) => StoreError::Contention,
            _ => StoreError::Other(self.to_string())
        }
    }
}

impl ToStoreError for QueryError {
    fn to_store_error(&self) -> StoreError {
        match self {
            QueryError::InternalServerError(_) => StoreError::Unavailable(self.to_string()),
            QueryError::ProvisionedThroughputExceededException(_) => StoreError::Ratelimit(self.to_string()),
            QueryError::ResourceNotFoundException(_) => StoreError::Unavailable(self.to_string()),
            QueryError::ThrottlingException(_) => StoreError::Ratelimit(self.to_string()),
            _ => StoreError::Other(self.to_string())
        }
    }
}

impl ToStoreError for TransactWriteItemsError {
    fn to_store_error(&self) -> StoreError {
        match self {
            TransactWriteItemsError::TransactionCanceledException(_) => StoreError::Other(format!("{} {:?}", self, self)),
            TransactWriteItemsError::IdempotentParameterMismatchException(_) => StoreError::Other(self.to_string()),
            TransactWriteItemsError::InternalServerError(_) => StoreError::Unavailable(self.to_string()),
            TransactWriteItemsError::ProvisionedThroughputExceededException(_) => StoreError::Ratelimit(self.to_string()),
            TransactWriteItemsError::ResourceNotFoundException(_) => StoreError::Unavailable(self.to_string()),
            TransactWriteItemsError::ThrottlingException(_) => StoreError::Ratelimit(self.to_string()),
            _ => StoreError::Other(format!("{} {:?}", self, self))
        }
    }
}
