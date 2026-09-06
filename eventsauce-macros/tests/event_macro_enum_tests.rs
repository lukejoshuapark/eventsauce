use std::assert_matches;

use eventsauce::{Event, StateError};
use eventsauce_macros::event;
use time::OffsetDateTime;
use uuid::Uuid;

#[event(user_id)]
pub struct UserCreatedEvent {
    pub first_name: String,
    pub last_name: String
}

#[event(user_id)]
pub struct UserNameUpdatedEvent {
    pub first_name: String,
    pub last_name: String
}

#[event]
pub enum UserEvent {
    Created(UserCreatedEvent),
    NameUpdated(UserNameUpdatedEvent)
}

#[test]
pub fn test_event_macro_enum_event_trait_delegates_to_creation_variant() {
    // Arrange.
    let id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let mut event: UserEvent = UserCreatedEvent::new(id, user_id, "John".to_string(), "Doe".to_string()).into();

    // Act and Assert.
    assert_eq!(event.id(), id);
    assert_eq!(event.stream_id(), user_id);
    assert_eq!(event.occurred_at(), None);
    assert_eq!(event.sequence_number(), None);

    let timestamp = OffsetDateTime::now_utc();
    event.set_occurred_at(timestamp);
    assert_eq!(event.occurred_at(), Some(timestamp));

    event.set_sequence_number(3);
    assert_eq!(event.sequence_number(), Some(3));
}

#[test]
pub fn test_event_macro_enum_event_trait_delegates_to_non_creation_variant() {
    // Arrange.
    let id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let event: UserEvent = UserNameUpdatedEvent::new(id, user_id, "Jackson".to_string(), "Smith".to_string()).into();

    // Act and Assert.
    assert_eq!(event.id(), id);
    assert_eq!(event.stream_id(), user_id);
}

#[test]
pub fn test_event_macro_enum_from_impls() {
    // Arrange.
    let id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let created = UserCreatedEvent::new(id, user_id, "John".to_string(), "Doe".to_string());
    let updated = UserNameUpdatedEvent::new(id, user_id, "Jackson".to_string(), "Smith".to_string());

    // Act.
    let created_event: UserEvent = created.into();
    let updated_event: UserEvent = updated.into();

    // Assert.
    assert_matches!(created_event, UserEvent::Created(_));
    assert_matches!(updated_event, UserEvent::NameUpdated(_));
}

#[test]
pub fn test_event_macro_enum_try_from_creation_event_success() {
    // Arrange.
    let id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let event: UserEvent = UserCreatedEvent::new(id, user_id, "John".to_string(), "Doe".to_string()).into();

    // Act.
    let creation_event: UserCreatedEvent = event.try_into().unwrap();

    // Assert.
    assert_eq!(creation_event.id, id);
    assert_eq!(creation_event.user_id, user_id);
}

#[test]
pub fn test_event_macro_enum_try_from_creation_event_failure() {
    // Arrange.
    let id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let event: UserEvent = UserNameUpdatedEvent::new(id, user_id, "Jackson".to_string(), "Smith".to_string()).into();

    // Act.
    let result: Result<UserCreatedEvent, StateError> = event.try_into();

    // Assert.
    assert_matches!(result.unwrap_err(), StateError::FirstEventIsNotCreationEvent);
}

#[test]
pub fn test_event_macro_enum_debug() {
    // Arrange.
    let id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let event: UserEvent = UserCreatedEvent::new(id, user_id, "John".to_string(), "Doe".to_string()).into();

    // Act.
    let debug_string = format!("{:?}", event);

    // Assert.
    assert!(debug_string.contains("Created"));
}
