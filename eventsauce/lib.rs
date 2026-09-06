//! `eventsauce` is a small event-sourcing library.
//!
//! An aggregate's state ([`State`]) is derived by replaying a stream of
//! [`Event`]s in order, starting from a creation event. [`Aggregate`] wraps a
//! state together with its version and any events that have not yet been
//! persisted, and the [`Store`] trait persists and retrieves aggregates from
//! a backing store. [`StateError`], [`AggregateError`], and [`StoreError`]
//! describe the ways these operations can fail.
//!
//! # Example
//!
//! ```
//! use eventsauce::{Aggregate, State};
//!
//! use uuid::Uuid;
//!
//! #[eventsauce::event(user_id)]
//! pub struct UserCreatedEvent {
//!     pub first_name: String,
//!     pub last_name: String
//! }
//!
//! #[eventsauce::event(user_id)]
//! pub struct UserNameUpdatedEvent {
//!     pub first_name: String,
//!     pub last_name: String
//! }
//!
//! #[eventsauce::event]
//! pub enum UserEvent {
//!     Created(UserCreatedEvent),
//!     NameUpdated(UserNameUpdatedEvent)
//! }
//!
//! pub struct UserState {
//!     pub user_id: Uuid,
//!     pub first_name: String,
//!     pub last_name: String
//! }
//!
//! impl State for UserState {
//!     type Event = UserEvent;
//!     type CreationEvent = UserCreatedEvent;
//!
//!     fn new(event: &Self::CreationEvent) -> Self {
//!         UserState {
//!             user_id: event.user_id,
//!             first_name: event.first_name.clone(),
//!             last_name: event.last_name.clone()
//!         }
//!     }
//!
//!     fn logical_version() -> u32 { 0 }
//!     fn stream_id(&self) -> Uuid { self.user_id }
//!
//!     fn apply(&mut self, event: &Self::Event) {
//!         match event {
//!             UserEvent::Created(event) => {
//!                 self.first_name = event.first_name.clone();
//!                 self.last_name = event.last_name.clone();
//!             },
//!             UserEvent::NameUpdated(event) => {
//!                 self.first_name = event.first_name.clone();
//!                 self.last_name = event.last_name.clone();
//!             }
//!         }
//!     }
//! }
//!
//! let event = UserCreatedEvent::new(Uuid::new_v4(), Uuid::new_v4(), "John".to_string(), "Doe".to_string());
//! let mut aggregate: Aggregate<UserState> = Aggregate::from_creation(event);
//!
//! aggregate.mutate(|user_id| UserNameUpdatedEvent::new(Uuid::new_v4(), user_id, "Jack".to_string(), "Smith".to_string()));
//! assert_eq!(aggregate.state().first_name, "Jack");
//! ```

mod aggregate_error;
mod aggregate;
mod concurrency_safe_persist;
mod event;
mod state_error;
mod state;
mod store_error;
mod store;

pub use aggregate_error::*;
pub use aggregate::*;
pub use concurrency_safe_persist::*;
pub use event::*;
pub use eventsauce_macros::event;
pub use state_error::*;
pub use state::*;
pub use store_error::*;
pub use store::*;
