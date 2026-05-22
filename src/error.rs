use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    /// Another node won the compare-and-swap race; the caller did not acquire leadership.
    #[error("condition check failed: another node holds the lock")]
    ConditionFailed,

    /// A Route53 API call failed for a non-contention reason.
    #[error("Route53 API error: {0}")]
    Api(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// The coordinator background task has already stopped.
    #[error("coordinator is shut down")]
    Shutdown,

    /// The lock record stored in Route53 is malformed.
    #[error("malformed lock record: {0}")]
    MalformedRecord(String),
}

pub(crate) fn api_err<E>(e: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    Error::Api(Box::new(e))
}
