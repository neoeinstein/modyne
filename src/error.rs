use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{
        delete_item::DeleteItemError, get_item::GetItemError, put_item::PutItemError,
        query::QueryError, scan::ScanError, transact_get_items::TransactGetItemsError,
        transact_write_items::TransactWriteItemsError, update_item::UpdateItemError,
    },
};

use crate::EntityTypeNameRef;

/// An error that occurred while interacting with DynamoDB
#[derive(Debug, thiserror::Error)]
#[repr(transparent)]
#[error(transparent)]
pub struct Error(Box<InnerError>);

impl Error {
    /// Returns true if the error is a conditional check failed exception
    ///
    /// See the [AWS documentation][AWS] for more information.
    ///
    /// [AWS]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html
    pub fn is_conditional_check_failed_exception(&self) -> bool {
        match &*self.0 {
            InnerError::PutItem(SdkError::ServiceError(e)) => {
                e.err().is_conditional_check_failed_exception()
            }
            InnerError::DeleteItem(SdkError::ServiceError(e)) => {
                e.err().is_conditional_check_failed_exception()
            }
            InnerError::UpdateItem(SdkError::ServiceError(e)) => {
                e.err().is_conditional_check_failed_exception()
            }
            InnerError::TransactWriteItems(SdkError::ServiceError(e)) => match e.err() {
                TransactWriteItemsError::TransactionCanceledException(e) => e
                    .cancellation_reasons
                    .iter()
                    .flatten()
                    .any(|r| r.code.as_deref() == Some("ConditionalCheckFailed")),
                _ => false,
            },
            _ => false,
        }
    }

    /// Returns true if the error is a provisioned throughput exceeded exception
    ///
    /// See the [AWS documentation][AWS] for more information.
    ///
    /// [AWS]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.html#ProvisionedThroughput
    pub fn is_provisioned_throughput_exceeded_exception(&self) -> bool {
        match &*self.0 {
            InnerError::GetItem(SdkError::ServiceError(e)) => {
                e.err().is_provisioned_throughput_exceeded_exception()
            }
            InnerError::Query(SdkError::ServiceError(e)) => {
                e.err().is_provisioned_throughput_exceeded_exception()
            }
            InnerError::Scan(SdkError::ServiceError(e)) => {
                e.err().is_provisioned_throughput_exceeded_exception()
            }
            InnerError::PutItem(SdkError::ServiceError(e)) => {
                e.err().is_provisioned_throughput_exceeded_exception()
            }
            InnerError::DeleteItem(SdkError::ServiceError(e)) => {
                e.err().is_provisioned_throughput_exceeded_exception()
            }
            InnerError::UpdateItem(SdkError::ServiceError(e)) => {
                e.err().is_provisioned_throughput_exceeded_exception()
            }
            InnerError::TransactGetItems(SdkError::ServiceError(e)) => match e.err() {
                TransactGetItemsError::TransactionCanceledException(e) => e
                    .cancellation_reasons
                    .iter()
                    .flatten()
                    .any(|r| r.code.as_deref() == Some("ProvisionedThroughputExceeded")),
                e => e.is_provisioned_throughput_exceeded_exception(),
            },
            InnerError::TransactWriteItems(SdkError::ServiceError(e)) => match e.err() {
                TransactWriteItemsError::TransactionCanceledException(e) => e
                    .cancellation_reasons
                    .iter()
                    .flatten()
                    .any(|r| r.code.as_deref() == Some("ProvisionedThroughputExceeded")),
                e => e.is_provisioned_throughput_exceeded_exception(),
            },
            _ => false,
        }
    }

    /// Returns true if the error is due to a request limit being exceeded
    ///
    /// See the [AWS documentation][AWS] for more information.
    ///
    /// [AWS]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
    pub fn is_request_limit_exceeded(&self) -> bool {
        match &*self.0 {
            InnerError::GetItem(SdkError::ServiceError(e)) => e.err().is_request_limit_exceeded(),
            InnerError::Query(SdkError::ServiceError(e)) => e.err().is_request_limit_exceeded(),
            InnerError::Scan(SdkError::ServiceError(e)) => e.err().is_request_limit_exceeded(),
            InnerError::PutItem(SdkError::ServiceError(e)) => e.err().is_request_limit_exceeded(),
            InnerError::DeleteItem(SdkError::ServiceError(e)) => {
                e.err().is_request_limit_exceeded()
            }
            InnerError::UpdateItem(SdkError::ServiceError(e)) => {
                e.err().is_request_limit_exceeded()
            }
            InnerError::TransactGetItems(SdkError::ServiceError(e)) => {
                e.err().is_request_limit_exceeded()
            }
            InnerError::TransactWriteItems(SdkError::ServiceError(e)) => {
                e.err().is_request_limit_exceeded()
            }
            _ => false,
        }
    }
}

impl<T> From<T> for Error
where
    T: Into<InnerError>,
{
    fn from(e: T) -> Self {
        Self(Box::new(e.into()))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("dynamodb repository error")]
pub(crate) enum InnerError {
    GetItem(#[from] SdkError<GetItemError>),
    Query(#[from] SdkError<QueryError>),
    Scan(#[from] SdkError<ScanError>),
    PutItem(#[from] SdkError<PutItemError>),
    DeleteItem(#[from] SdkError<DeleteItemError>),
    UpdateItem(#[from] SdkError<UpdateItemError>),
    TransactGetItems(#[from] SdkError<TransactGetItemsError>),
    TransactWriteItems(#[from] SdkError<TransactWriteItemsError>),
    ItemDeserialization(#[from] ItemDeserializationError),
    MissingEntityType(#[from] MissingEntityTypeError),
}

#[derive(Debug, thiserror::Error)]
#[error("failed to deserialize item of type `{entity_type}`")]
pub(crate) struct ItemDeserializationError {
    entity_type: &'static EntityTypeNameRef,
    source: serde_dynamo::Error,
}

impl ItemDeserializationError {
    #[inline]
    pub(crate) fn new(
        entity_type: &'static EntityTypeNameRef,
        source: serde_dynamo::Error,
    ) -> Self {
        Self {
            entity_type,
            source,
        }
    }
}

/// An error retrieving the entity type for a DynamoDB item
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MissingEntityTypeError {
    /// The entity type attribute was not found on the item
    #[error("entity type attribute is missing from the item")]
    AttributeNotFound,

    /// The entity type attribute was found, but was malformed and could not be extracted
    #[error("entity type attribute value is malformed and could not be extracted from the item")]
    MalformedAttributeValue(#[source] Option<Box<dyn std::error::Error + Send + Sync>>),
}
