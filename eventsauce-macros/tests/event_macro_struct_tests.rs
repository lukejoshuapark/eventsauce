use eventsauce::Event;
use eventsauce_macros::event;
use uuid::Uuid;

#[event(user_id)]
pub struct UserCreatedEvent {
    pub first_name: String,
    pub last_name: String
}

#[test]
pub fn test_event_macro_new() {
    // Arrange.
    let id = Uuid::new_v4();
    let user_id = Uuid::new_v4();

    // Act.
    let event = UserCreatedEvent::new(id, user_id, "John".to_string(), "Doe".to_string());

    // Assert.
    assert_eq!(event.id, id);
    assert_eq!(event.user_id, user_id);
    assert_eq!(event.occurred_at, None);
    assert_eq!(event.sequence_number, None);
    assert_eq!(event.first_name, "John");
    assert_eq!(event.last_name, "Doe");
}

#[test]
pub fn test_event_macro_event_trait() {
    // Arrange.
    let id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let mut event = UserCreatedEvent::new(id, user_id, "John".to_string(), "Doe".to_string());

    // Act and Assert.
    assert_eq!(event.id(), id);
    assert_eq!(event.stream_id(), user_id);
    assert_eq!(event.occurred_at(), None);
    assert_eq!(event.sequence_number(), None);

    let timestamp = time::OffsetDateTime::now_utc();
    event.set_occurred_at(timestamp);
    assert_eq!(event.occurred_at(), Some(timestamp));

    event.set_sequence_number(5);
    assert_eq!(event.sequence_number(), Some(5));
}
