use eventsauce::{Aggregate, Event, State, Store, StoreError, concurrency_safe_persist};
use eventsauce_store_dynamodb::DynamoDbStore;

use std::convert::Infallible;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[tokio::test]
pub async fn test_dynamodb_store_can_persist_and_retrieve_user_aggregate() {
    // Arrange.
    let store = create_store().await;
    let user_id = Uuid::new_v4();
    let mut user1 = UserAggregate::new(user_id, "John".to_string(), "Doe".to_string(), "john.doe@gmail.com".to_string());
    user1.update_name("Jackson".to_string(), "Smith".to_string());

    // Act.
    store.persist(user1).await.unwrap();
    let user2: UserAggregate = store.retrieve(user_id).await.unwrap().unwrap();

    // Assert.
    let user2_state = user2.state();
    assert_eq!(user2_state.user_id, user_id);
    assert_eq!(user2_state.first_name, "Jackson");
    assert_eq!(user2_state.last_name, "Smith");
}

#[tokio::test]
pub async fn test_dynamodb_store_can_retrieve_user_aggregate_from_events_alone() {
    // Arrange.
    let store = create_store().await;
    let user_id = Uuid::new_v4();
    let mut user1 = UserAggregate::new(user_id, "John".to_string(), "Doe".to_string(), "john.doe@gmail.com".to_string());
    user1.update_name("Jackson".to_string(), "Smith".to_string());

    // Act.
    store.persist(user1).await.unwrap();
    delete_state(user_id).await;

    let user2: UserAggregate = store.retrieve(user_id).await.unwrap().unwrap();

    // Assert.
    let user2_state = user2.state();
    assert_eq!(user2_state.user_id, user_id);
    assert_eq!(user2_state.first_name, "Jackson");
    assert_eq!(user2_state.last_name, "Smith");
}

#[tokio::test]
pub async fn test_dynamodb_store_correctly_detects_contention_when_persisting() {
    // Arrange.
    let store = create_store().await;
    let user_id = Uuid::new_v4();
    let user = UserAggregate::new(user_id, "John".to_string(), "Doe".to_string(), "john.doe@gmail.com".to_string());
    store.persist(user).await.unwrap();

    let mut user_a: UserAggregate = store.retrieve(user_id).await.unwrap().unwrap();
    let mut user_b: UserAggregate = store.retrieve(user_id).await.unwrap().unwrap();

    user_a.update_name("Jackson".to_string(), "Smith".to_string());
    user_b.update_name("Jane".to_string(), "Doe".to_string());

    // Act.
    store.persist(user_a).await.unwrap();
    let result = store.persist(user_b).await;

    // Assert.
    assert!(matches!(result, Err(StoreError::Contention)));
}

#[tokio::test]
pub async fn test_dynamodb_store_correctly_detects_sequence_number_gap() {
    // Arrange.
    let store = create_store().await;
    let (_, state, version) = create_and_persist_user_with_updates(&store).await;

    // Act.
    let mut aggregate = UserAggregate::from_state(state, version + 1);
    aggregate.update_name("Gap".to_string(), "Test".to_string());
    let result = store.persist(aggregate).await;

    // Assert.
    assert!(matches!(result, Err(StoreError::MissingPrecedingEvent { sequence_number, .. }) if sequence_number == version + 1));
}

#[tokio::test]
pub async fn test_dynamodb_store_correctly_detects_sequence_number_duplicates() {
    // Arrange.
    let store = create_store().await;
    let (_, state, version) = create_and_persist_user_with_updates(&store).await;

    // Act.
    let mut aggregate = UserAggregate::from_state(state, version - 2);
    aggregate.update_name("Duplicate".to_string(), "One".to_string());
    aggregate.update_name("Duplicate".to_string(), "Two".to_string());
    let result = store.persist(aggregate).await;

    // Assert.
    assert!(matches!(result, Err(StoreError::Contention)));
}

#[tokio::test]
pub async fn test_dynamodb_store_concurrency_safe_persist_recovers_from_contention() {
    // Arrange.
    let store = create_store().await;
    let user_id = Uuid::new_v4();
    let user = UserAggregate::new(user_id, "John".to_string(), "Doe".to_string(), "john.doe@gmail.com".to_string());
    store.persist(user).await.unwrap();

    let interloper_has_run = AtomicBool::new(false);
    let store_ref = &store;
    let interloper_has_run_ref = &interloper_has_run;

    // Act.
    let result = concurrency_safe_persist(user_id, create_store().await, async move |user_b: &mut UserAggregate| {
        if !interloper_has_run_ref.swap(true, Ordering::SeqCst) {
            let mut user_a: UserAggregate = store_ref.retrieve(user_id).await.unwrap().unwrap();
            user_a.update_name("Jane".to_string(), "Doe".to_string());
            store_ref.persist(user_a).await.unwrap();
        }

        user_b.update_name("Jackson".to_string(), "Smith".to_string());
        Ok::<(), Infallible>(())
    }).await;

    // Assert.
    assert!(result.is_ok());

    let user2: UserAggregate = store.retrieve(user_id).await.unwrap().unwrap();
    let user2_state = user2.state();
    assert_eq!(user2_state.first_name, "Jackson");
    assert_eq!(user2_state.last_name, "Smith");
}

#[tokio::test]
pub async fn test_dynamodb_store_retrieve_events_returns_newest_events_first() {
    // Arrange.
    let store = create_store().await;
    let user_id = create_and_persist_user_with_event_count(&store, 5).await;

    // Act.
    let page = store.retrieve_events(user_id, None, NonZeroU32::new(2).unwrap()).await.unwrap().unwrap();

    // Assert.
    assert_eq!(sequence_numbers(&page.events), vec![4, 3]);
    assert_eq!(page.next, Some(3));
}

#[tokio::test]
pub async fn test_dynamodb_store_retrieve_events_pages_through_whole_stream() {
    // Arrange.
    let store = create_store().await;
    let user_id = create_and_persist_user_with_event_count(&store, 5).await;

    // Act.
    let mut pages = vec![];
    let mut before = None;

    loop {
        let page = store.retrieve_events(user_id, before, NonZeroU32::new(2).unwrap()).await.unwrap().unwrap();
        before = page.next;
        pages.push(sequence_numbers(&page.events));

        if before.is_none() {
            break;
        }
    }

    // Assert.
    assert_eq!(pages, vec![vec![4, 3], vec![2, 1], vec![0]]);
}

#[tokio::test]
pub async fn test_dynamodb_store_retrieve_events_returns_whole_stream_when_limit_exceeds_length() {
    // Arrange.
    let store = create_store().await;
    let user_id = create_and_persist_user_with_event_count(&store, 5).await;

    // Act.
    let page = store.retrieve_events(user_id, None, NonZeroU32::new(10).unwrap()).await.unwrap().unwrap();

    // Assert.
    assert_eq!(sequence_numbers(&page.events), vec![4, 3, 2, 1, 0]);
    assert_eq!(page.next, None);
}

#[tokio::test]
pub async fn test_dynamodb_store_retrieve_events_treats_before_past_end_of_stream_as_newest() {
    // Arrange.
    let store = create_store().await;
    let user_id = create_and_persist_user_with_event_count(&store, 5).await;

    // Act.
    let page = store.retrieve_events(user_id, Some(100), NonZeroU32::new(2).unwrap()).await.unwrap().unwrap();

    // Assert.
    assert_eq!(sequence_numbers(&page.events), vec![4, 3]);
    assert_eq!(page.next, Some(3));
}

#[tokio::test]
pub async fn test_dynamodb_store_retrieve_events_returns_empty_page_before_creation_event() {
    // Arrange.
    let store = create_store().await;
    let user_id = create_and_persist_user_with_event_count(&store, 5).await;

    // Act.
    let page = store.retrieve_events(user_id, Some(0), NonZeroU32::new(2).unwrap()).await.unwrap().unwrap();

    // Assert.
    assert!(page.events.is_empty());
    assert_eq!(page.next, None);
}

#[tokio::test]
pub async fn test_dynamodb_store_retrieve_events_fills_page_beyond_dynamodb_response_size_limit() {
    // Arrange.
    let store = create_store().await;
    let user_id = Uuid::new_v4();
    let large_name = "x".repeat(300 * 1024);
    let mut user = UserAggregate::new(user_id, large_name.clone(), "Doe".to_string(), "john.doe@gmail.com".to_string());
    for _ in 1..6 {
        user.update_name(large_name.clone(), "Doe".to_string());
    }

    store.persist(user).await.unwrap();

    // Act. Six ~300KB events can't fit in one 1MB Query response.
    let page = store.retrieve_events(user_id, None, NonZeroU32::new(5).unwrap()).await.unwrap().unwrap();

    // Assert.
    assert_eq!(sequence_numbers(&page.events), vec![5, 4, 3, 2, 1]);
    assert_eq!(page.next, Some(1));
}

#[tokio::test]
pub async fn test_dynamodb_store_retrieve_events_returns_none_for_unknown_stream() {
    // Arrange.
    let store = create_store().await;

    // Act.
    let first_page = store.retrieve_events(Uuid::new_v4(), None, NonZeroU32::new(2).unwrap()).await.unwrap();
    let later_page = store.retrieve_events(Uuid::new_v4(), Some(3), NonZeroU32::new(2).unwrap()).await.unwrap();

    // Assert.
    assert!(first_page.is_none());
    assert!(later_page.is_none());
}

// -----------------------------------------------------------------------------

pub async fn create_store() -> DynamoDbStore<UserState> {
    unsafe {
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
        std::env::set_var("AWS_ACCESS_KEY_ID", "ABC123");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "321CBA");
    }

    let shared_config = aws_config::load_from_env().await;
    let dynamodb_config = aws_sdk_dynamodb::config::Builder::from(&shared_config)
        .endpoint_url("http://host.docker.internal:8993")
        .build();

    let client = aws_sdk_dynamodb::Client::from_conf(dynamodb_config);
    DynamoDbStore::new(String::from("test"), client)
}

pub async fn delete_state(stream_id: Uuid) {
    let shared_config = aws_config::load_from_env().await;
    let dynamodb_config = aws_sdk_dynamodb::config::Builder::from(&shared_config)
        .endpoint_url("http://host.docker.internal:8993")
        .build();

    let client = aws_sdk_dynamodb::Client::from_conf(dynamodb_config);

    client
        .delete_item()
        .table_name("test")
        .key("StreamId", AttributeValue::S(format!("STREAM#{}", stream_id)))
        .key("Name", AttributeValue::S(String::from("STATE#0")))
        .send()
        .await
        .unwrap();
}

pub async fn create_and_persist_user_with_updates(store: &DynamoDbStore<UserState>) -> (Uuid, UserState, u32) {
    let user_id = Uuid::new_v4();
    let mut user = UserAggregate::new(user_id, "John".to_string(), "Doe".to_string(), "john.doe@gmail.com".to_string());
    user.update_name("Jackson".to_string(), "Smith".to_string());
    user.update_name("Jack".to_string(), "Smyth".to_string());
    store.persist(user).await.unwrap();

    let persisted: UserAggregate = store.retrieve(user_id).await.unwrap().unwrap();
    let (state, _, version) = persisted.take();
    (user_id, state, version)
}

pub async fn create_and_persist_user_with_event_count(store: &DynamoDbStore<UserState>, event_count: usize) -> Uuid {
    let user_id = Uuid::new_v4();
    let mut user = UserAggregate::new(user_id, "John".to_string(), "Doe".to_string(), "john.doe@gmail.com".to_string());
    for i in 1..event_count {
        user.update_name(format!("John{}", i), "Doe".to_string());
    }

    store.persist(user).await.unwrap();
    user_id
}

pub fn sequence_numbers(events: &[UserEvent]) -> Vec<u32> {
    events.iter().map(|event| event.sequence_number().unwrap()).collect()
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

#[derive(Deserialize, Serialize, Debug)]
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
