# eventsauce-store-dynamodb

A DynamoDB-backed `Store` implementation for
[`eventsauce`](https://docs.rs/eventsauce).

Your DynamoDB table must use this arrangement:

- Partition key: `StreamId` (String)
- Sort key: `Name` (String)

`DynamoDbStore`:

- `persist` writes the new state and events in a single DynamoDB
  transaction, capped at DynamoDB's 100-item transaction limit: up to 99
  events for a new stream, or 98 if the stream already exists, since
  persisting to an existing stream adds a condition check on the preceding
  event as an extra transaction item.

- `retrieve` prefers reading the stored state directly when its logical
  version matches the aggregate's state, falling back to rebuilding from
  events otherwise. If rebuilt from events, the state is persisted back for
  next time.
