# eventsauce-macros

Procedural macros for [`eventsauce`](https://docs.rs/eventsauce).

`#[event]` generates an `eventsauce::Event` implementation for a struct or
enum. With the `serde` feature enabled, `Serialize` and `Deserialize` are
also derived.

Apply to Event structs (passing an appropriate name for the stream ID) to
get your `Event` implementation for free.

```rust,ignore
#[eventsauce::event(user_id)]
pub struct UserCreatedEvent {
    pub first_name: String,
    pub last_name: String,
    pub email_address: String
}
```

Apply to Event enums to get necessary `From` and `TryFrom` implementations.

```rust,ignore
#[eventsauce::event]
pub enum UserEvent {
    Created(UserCreatedEvent),
    NameUpdated(UserNameUpdatedEvent)
}
```

Using this on Event structs and the grouped Event enum is the recommended
pattern.
