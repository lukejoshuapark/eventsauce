# eventsauce

A small event-sourcing library, split across a few crates:

- [`eventsauce`](eventsauce) — the core library: `Event`, `State`, `Aggregate`, and the `Store` trait.
- [`eventsauce-macros`](eventsauce-macros) — the `#[event]` macro.
- [`eventsauce-store-dynamodb`](eventsauce-store-dynamodb) — a DynamoDB-backed `Store`.
- [`eventsauce-store-memory`](eventsauce-store-memory) — an in-memory `Store`, for tests and examples.

See each crate's own README for details.
