# eventsauce-store-memory

An in-memory `Store` implementation for
[`eventsauce`](https://docs.rs/eventsauce), intended for tests and examples.

Events are serialized with `serde_json` into a single
`HashMap<String, String>`, keyed by stream ID and sequence number. State is
never persisted; every retrieve rebuilds the aggregate by replaying its
events.

`MemoryStore`:

- `persist` appends the new events under a lock, failing with
  `StoreError::Contention` if any of their sequence numbers have already
  been taken, or `StoreError::MissingPrecedingEvent` if the event they were
  meant to follow is not present.

- `retrieve` always rebuilds the aggregate from its events; there is no
  persisted state to read directly.

- `retrieve_events` pages through a stream's events newest first. It scans
  and sorts every key in the store on each call, which is fine at test
  scale.

Since `MemoryStore` owns its data and can't cheaply be cloned into a new
handle onto the same store the way a networked client can, `&MemoryStore<S>`
also implements `Store<S>`, so a borrowed store can be used anywhere an
owned one is expected (e.g. `concurrency_safe_persist`).
