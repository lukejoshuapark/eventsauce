//! Retrying an aggregate mutation until it persists without hitting stream
//! contention.

use crate::{Aggregate, State, Store, StoreError};

use std::error::Error;

use uuid::Uuid;

const MAXIMUM_ATTEMPTS: u32 = 10;

/// Errors that can occur while running [`concurrency_safe_persist`].
#[derive(Debug, thiserror::Error)]
pub enum ConcurrencySafePersistError<E : Error> {
    /// `stream_id` does not identify an existing stream in the store.
    #[error("The specified stream does not exist")]
    StreamDoesNotExist,

    /// Persisting the mutation lost to contention on every one of
    /// `MAXIMUM_ATTEMPTS` attempts.
    #[error("There were {} attempts to persist the mutation, but each failed due to contention", MAXIMUM_ATTEMPTS)]
    RetryLimitExceeded,

    /// The mutation closure returned an error, so no persist was attempted.
    #[error("The mutation function itself failed: {0}")]
    MutationError(#[from] E),

    /// Retrieving or persisting the aggregate failed for a reason other
    /// than contention.
    #[error("The store failed with an error that was not related to contention: {0}")]
    StoreError(#[source] StoreError)
}

/// Retrieves the aggregate for `stream_id`, applies `f` to it, and persists
/// the result, retrying from scratch whenever persistence fails with
/// [`StoreError::Contention`].
///
/// Because the aggregate is re-retrieved on every attempt, `f` may be
/// invoked more than once and should be free of side effects other than
/// mutating the aggregate it is given.
///
/// # Errors
///
/// Returns [`ConcurrencySafePersistError::StreamDoesNotExist`] if `stream_id`
/// does not identify an existing stream, [`ConcurrencySafePersistError::MutationError`]
/// if `f` fails, [`ConcurrencySafePersistError::StoreError`] if retrieving or
/// persisting the aggregate fails for a reason other than contention, or
/// [`ConcurrencySafePersistError::RetryLimitExceeded`] if `MAXIMUM_ATTEMPTS`
/// consecutive attempts all lost to contention.
pub async fn concurrency_safe_persist<S, T, E, C>(stream_id: Uuid, store: T, f: C) -> Result<(), ConcurrencySafePersistError<E>>
where
    S : State,
    T : Store<S>,
    E : Error,
    C : AsyncFn(&mut Aggregate<S>) -> Result<(), E>
{
    let mut attempt_count = 0;

    loop {
        attempt_count += 1;
        if attempt_count > MAXIMUM_ATTEMPTS {
            return Err(ConcurrencySafePersistError::RetryLimitExceeded);
        }

        let mut aggregate = match store.retrieve(stream_id).await {
            Ok(Some(x)) => x,
            Ok(None) => return Err(ConcurrencySafePersistError::StreamDoesNotExist),
            Err(err) => return Err(ConcurrencySafePersistError::StoreError(err))
        };

        if let Err(err) = f(&mut aggregate).await {
            return Err(ConcurrencySafePersistError::MutationError(err))
        }

        match store.persist(aggregate).await {
            Ok(_) => return Ok(()),
            Err(StoreError::Contention) => continue,
            Err(err) => return Err(ConcurrencySafePersistError::StoreError(err))
        }
    }
}
