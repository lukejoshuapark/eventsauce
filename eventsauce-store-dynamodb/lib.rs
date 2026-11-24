//! A DynamoDB-backed [`Store`](eventsauce::Store) implementation for
//! [`eventsauce`](https://docs.rs/eventsauce).
//!
//! Your DynamoDB table must use this arrangement:
//!
//! - Partition key: `StreamId` (String)
//! - Sort key: `Name` (String)

mod create_condition_check_for_head;
mod create_transact_write_item_for_event;
mod create_transact_write_item_for_head;
mod create_transact_write_item_for_state;
mod dynamodb_store;
mod format_event_sort_key;
mod head_sort_key;
mod persist_state;
mod retrieve_event_page;
mod retrieve_state_and_head;
mod retrieve_stream_events;
mod to_store_error;
mod to_store_error_for_transaction;

pub(crate) use create_condition_check_for_head::*;
pub(crate) use create_transact_write_item_for_event::*;
pub(crate) use create_transact_write_item_for_head::*;
pub(crate) use create_transact_write_item_for_state::*;
pub use dynamodb_store::*;
pub(crate) use format_event_sort_key::*;
pub(crate) use head_sort_key::*;
pub(crate) use persist_state::*;
pub(crate) use retrieve_event_page::*;
pub(crate) use retrieve_state_and_head::*;
pub(crate) use retrieve_stream_events::*;
pub(crate) use to_store_error::*;
pub(crate) use to_store_error_for_transaction::*;
