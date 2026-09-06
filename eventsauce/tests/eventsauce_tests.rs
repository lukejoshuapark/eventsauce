use eventsauce::{Aggregate, AggregateError, State};

use std::assert_matches;

use time::OffsetDateTime;
use uuid::Uuid;

#[test]
pub fn test_user_aggregate_from_creation() {
    // Arrange.
    let user_id = Uuid::new_v4();
    let aggregate = UserAggregate::new(user_id, "John".to_string(), "Doe".to_string(), "john.doe@gmail.com".to_string());

    // Act.
    let (state, pending_events, sequence_number) = aggregate.take();

    // Assert.
    assert_eq!(state.user_id, user_id);
    assert_eq!(state.first_name, "John");
    assert_eq!(state.last_name, "Doe");
    assert_eq!(state.email_address, "john.doe@gmail.com");
    assert_eq!(pending_events.len(), 1);
    assert_eq!(sequence_number, 0);
}

#[test]
#[should_panic(expected = "This creation event has previously been persisted")]
pub fn test_user_aggregate_from_creation_with_assigned_sequence_number() {
    // Arrange.
    let user_id = Uuid::new_v4();
    let creation_event = UserCreatedEvent {
        id: Uuid::new_v4(),
        user_id,
        occurred_at: None,
        sequence_number: Some(0),
        first_name: "John".to_string(),
        last_name: "Doe".to_string(),
        email_address: "john.doe@gmail.com".to_string()
    };

    // Act and Assert.
    UserAggregate::from_creation(creation_event);
}

#[test]
pub fn test_user_aggregate_from_state() {
    // Arrange.
    let user_id = Uuid::new_v4();
    let state = UserState {
        user_id,
        first_name: "John".to_string(),
        last_name: "Doe".to_string(),
        email_address: "john.doe@gmail.com".to_string()
    };

    let aggregate = UserAggregate::from_state(state, 1);

    // Act.
    let (state, pending_events, sequence_number) = aggregate.take();

    // Assert.
    assert_eq!(state.user_id, user_id);
    assert_eq!(state.first_name, "John");
    assert_eq!(state.last_name, "Doe");
    assert_eq!(state.email_address, "john.doe@gmail.com");
    assert_eq!(pending_events.len(), 0);
    assert_eq!(sequence_number, 1);
}

#[test]
pub fn test_user_aggregate_from_state_then_mutation() {
    // Arrange.
    let user_id = Uuid::new_v4();
    let state = UserState {
        user_id,
        first_name: "John".to_string(),
        last_name: "Doe".to_string(),
        email_address: "john.doe@gmail.com".to_string()
    };

    let mut aggregate = UserAggregate::from_state(state, 1);

    // Act.
    aggregate.update_name("Jackson".to_string(), "Smith".to_string());
    let (state, pending_events, sequence_number) = aggregate.take();

    // Assert.
    assert_eq!(state.user_id, user_id);
    assert_eq!(state.first_name, "Jackson");
    assert_eq!(state.last_name, "Smith");
    assert_eq!(state.email_address, "john.doe@gmail.com");
    assert_eq!(pending_events.len(), 1);
    assert_eq!(sequence_number, 2);
}

#[test]
pub fn test_user_aggregate_from_events() {
    // Arrange.
    let user_id = Uuid::new_v4();
    let events = vec![
        UserEvent::Created(UserCreatedEvent {
            id: Uuid::new_v4(),
            user_id,
            occurred_at: Some(OffsetDateTime::now_utc()),
            sequence_number: Some(0),
            first_name: "John".to_string(),
            last_name: "Doe".to_string(),
            email_address: "john.doe@gmail.com".to_string()
        }),
        UserEvent::NameUpdated(UserNameUpdatedEvent {
            id: Uuid::new_v4(),
            user_id,
            occurred_at: Some(OffsetDateTime::now_utc()),
            sequence_number: Some(1),
            first_name: "Jackson".to_string(),
            last_name: "Smith".to_string()
        })
    ];

    let aggregate = UserAggregate::from_events(events).unwrap();

    // Act.
    let (state, pending_events, version) = aggregate.take();

    // Assert.
    assert_eq!(state.user_id, user_id);
    assert_eq!(state.first_name, "Jackson");
    assert_eq!(state.last_name, "Smith");
    assert_eq!(state.email_address, "john.doe@gmail.com");
    assert_eq!(pending_events.len(), 0);
    assert_eq!(version, 1);
}

#[test]
pub fn test_user_aggregate_from_events_creation_event_without_sequence_number() {
    // Arrange.
    let user_id = Uuid::new_v4();
    let events = vec![
        UserEvent::Created(UserCreatedEvent {
            id: Uuid::new_v4(),
            user_id,
            occurred_at: Some(OffsetDateTime::now_utc()),
            sequence_number: None,
            first_name: "John".to_string(),
            last_name: "Doe".to_string(),
            email_address: "john.doe@gmail.com".to_string()
        }),
        UserEvent::NameUpdated(UserNameUpdatedEvent {
            id: Uuid::new_v4(),
            user_id,
            occurred_at: Some(OffsetDateTime::now_utc()),
            sequence_number: Some(1),
            first_name: "Jackson".to_string(),
            last_name: "Smith".to_string()
        })
    ];

    // Act.
    let result = UserAggregate::from_events(events).unwrap_err();

    // Assert.
    assert_matches!(result, AggregateError::PersistedEventHasNoSequenceNumber);
}

#[test]
pub fn test_user_aggregate_from_events_creation_event_sequence_number_not_zero() {
    // Arrange.
    let user_id = Uuid::new_v4();
    let events = vec![
        UserEvent::Created(UserCreatedEvent {
            id: Uuid::new_v4(),
            user_id,
            occurred_at: Some(OffsetDateTime::now_utc()),
            sequence_number: Some(1),
            first_name: "John".to_string(),
            last_name: "Doe".to_string(),
            email_address: "john.doe@gmail.com".to_string()
        }),
        UserEvent::NameUpdated(UserNameUpdatedEvent {
            id: Uuid::new_v4(),
            user_id,
            occurred_at: Some(OffsetDateTime::now_utc()),
            sequence_number: Some(1),
            first_name: "Jackson".to_string(),
            last_name: "Smith".to_string()
        })
    ];

    // Act.
    let result = UserAggregate::from_events(events).unwrap_err();

    // Assert.
    assert_matches!(result, AggregateError::UnexpectedSequenceNumber { expected: 0, actual: 1 });
}

#[test]
pub fn test_user_aggregate_from_events_subsequent_event_without_sequence_number() {
    // Arrange.
    let user_id = Uuid::new_v4();
    let events = vec![
        UserEvent::Created(UserCreatedEvent {
            id: Uuid::new_v4(),
            user_id,
            occurred_at: Some(OffsetDateTime::now_utc()),
            sequence_number: Some(0),
            first_name: "John".to_string(),
            last_name: "Doe".to_string(),
            email_address: "john.doe@gmail.com".to_string()
        }),
        UserEvent::NameUpdated(UserNameUpdatedEvent {
            id: Uuid::new_v4(),
            user_id,
            occurred_at: Some(OffsetDateTime::now_utc()),
            sequence_number: None,
            first_name: "Jackson".to_string(),
            last_name: "Smith".to_string()
        })
    ];

    // Act.
    let result = UserAggregate::from_events(events).unwrap_err();

    // Assert.
    assert_matches!(result, AggregateError::PersistedEventHasNoSequenceNumber);
}

#[test]
pub fn test_user_aggregate_from_events_subsequent_event_sequence_number_not_in_sequence() {
    // Arrange.
    let user_id = Uuid::new_v4();
    let events = vec![
        UserEvent::Created(UserCreatedEvent {
            id: Uuid::new_v4(),
            user_id,
            occurred_at: Some(OffsetDateTime::now_utc()),
            sequence_number: Some(0),
            first_name: "John".to_string(),
            last_name: "Doe".to_string(),
            email_address: "john.doe@gmail.com".to_string()
        }),
        UserEvent::NameUpdated(UserNameUpdatedEvent {
            id: Uuid::new_v4(),
            user_id,
            occurred_at: Some(OffsetDateTime::now_utc()),
            sequence_number: Some(2),
            first_name: "Jackson".to_string(),
            last_name: "Smith".to_string()
        })
    ];

    // Act.
    let result = UserAggregate::from_events(events).unwrap_err();

    // Assert.
    assert_matches!(result, AggregateError::UnexpectedSequenceNumber { expected: 1, actual: 2 });
}

// -----------------------------------------------------------------------------

pub type UserAggregate = Aggregate<UserState>;

pub trait UserAggregateMethods {
    fn new(user_id: Uuid, first_name: String, last_name: String, email_address: String) -> Self;
    fn update_name(&mut self, first_name: String, last_name: String);
}

impl UserAggregateMethods for Aggregate<UserState> {
    fn new(user_id: Uuid, first_name: String, last_name: String, email_address: String) -> Self {
        let event = UserCreatedEvent {
            id: Uuid::new_v4(),
            user_id,
            occurred_at: None,
            sequence_number: None,
            first_name,
            last_name,
            email_address
        };

        Aggregate::from_creation(event)
    }

    fn update_name(&mut self, first_name: String, last_name: String) {
        self.mutate(|user_id| {
            UserNameUpdatedEvent {
                id: Uuid::new_v4(),
                user_id,
                occurred_at: None,
                sequence_number: None,
                first_name,
                last_name
            }
        });
    }
}

#[derive(Debug)]
pub struct UserState {
    pub user_id: Uuid,
    pub first_name: String,
    pub last_name: String,
    pub email_address: String
}

impl State for UserState {
    type Event = UserEvent;
    type CreationEvent = UserCreatedEvent;

    fn new(event: &Self::CreationEvent) -> Self {
        UserState {
            user_id: event.user_id,
            first_name: event.first_name.clone(),
            last_name: event.last_name.clone(),
            email_address: event.email_address.clone()
        }
    }

    fn logical_version() -> u32 { 0 }

    fn stream_id(&self) -> Uuid {
        self.user_id
    }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            UserEvent::Created(event) => {
                self.user_id = event.user_id;
                self.first_name = event.first_name.clone();
                self.last_name = event.last_name.clone();
                self.email_address = event.email_address.clone();
            },
            UserEvent::NameUpdated(event) => {
                self.first_name = event.first_name.clone();
                self.last_name = event.last_name.clone();
            }
        }
    }
}

#[eventsauce_macros::event(user_id)]
pub struct UserCreatedEvent {
    pub first_name: String,
    pub last_name: String,
    pub email_address: String
}

#[eventsauce_macros::event(user_id)]
pub struct UserNameUpdatedEvent {
    pub first_name: String,
    pub last_name: String
}

#[eventsauce_macros::event]
pub enum UserEvent {
    Created(UserCreatedEvent),
    NameUpdated(UserNameUpdatedEvent)
}
