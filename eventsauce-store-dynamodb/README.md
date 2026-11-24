# eventsauce-store-dynamodb

A DynamoDB-backed `Store` implementation for
[`eventsauce`](https://docs.rs/eventsauce).

Your DynamoDB table must use this arrangement:

- Partition key: `StreamId` (String)
- Sort key: `Name` (String)

`DynamoDbStore`:

Each stream has a head item recording the sequence number of its newest
event, which every persist updates alongside the events it writes.

- `persist` writes the new events, state and head in a single DynamoDB
  transaction, conditional on the head being where the aggregate left it.
  With the head and state taking two of DynamoDB's 100 transaction items,
  up to 98 events can be persisted at once.

- `retrieve` reads the stored state for the aggregate's logical version and
  the stream's head together. If the state is behind the head (e.g. because
  code running a different logical version persisted events during a
  rollout), the events after it are applied. If there is no stored state,
  the aggregate is rebuilt from events and the state is persisted back for
  next time.

- `retrieve_events` reads a page with a descending `Query` on the stream's
  partition, so its cost depends on the page size, not the stream's length.
  If DynamoDB's 1MB response cap cuts a response short, it keeps querying
  until the page is full.
