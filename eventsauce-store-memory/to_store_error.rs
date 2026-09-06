use eventsauce::StoreError;

pub fn to_store_error(error: serde_json::Error) -> StoreError {
    StoreError::Serialization(error.to_string())
}
