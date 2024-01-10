//! Models for interacting with DynamoDB

use std::{collections::HashMap, fmt, marker::PhantomData};

use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{
        batch_get_item::{BatchGetItemError, BatchGetItemOutput},
        batch_write_item::{BatchWriteItemError, BatchWriteItemOutput},
        delete_item::{DeleteItemError, DeleteItemOutput},
        get_item::{GetItemError, GetItemOutput},
        put_item::{PutItemError, PutItemOutput},
        query::{QueryError, QueryOutput},
        scan::{ScanError, ScanOutput},
        transact_get_items::{TransactGetItemsError, TransactGetItemsOutput},
        transact_write_items::{TransactWriteItemsError, TransactWriteItemsOutput},
        update_item::{UpdateItemError, UpdateItemOutput},
    },
    types::{
        AttributeValue, ConsumedCapacity, KeysAndAttributes, ReturnConsumedCapacity, ReturnValue,
        ReturnValuesOnConditionCheckFailure, Select,
    },
};
use tracing::{field, Instrument};

use crate::{expr, keys, Item, Table};

/// A builder for get item operations
#[derive(Debug, Clone)]
#[must_use]
pub struct Get {
    projection: Option<expr::StaticProjection>,
    key: Item,
}

impl Get {
    /// Prepare a get item operation
    #[inline]
    pub fn new(key: Item) -> Self {
        Self {
            key,
            projection: None,
        }
    }

    /// Specify a projection expression
    #[inline]
    pub fn projection(mut self, projection: expr::StaticProjection) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Executes a single item get request against the given table
    ///
    /// This function executes the operation with eventual consistency
    pub async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<GetItemOutput, SdkError<GetItemError>> {
        GetOne {
            inner: self,
            consistent_read: None,
        }
        .execute(table)
        .await
    }

    /// Executes a single item get request against the given table with
    /// a specific read consistency
    pub async fn execute_with_consistency<T: Table>(
        self,
        table: &T,
        consistent_read: bool,
    ) -> Result<GetItemOutput, SdkError<GetItemError>> {
        GetOne {
            inner: self,
            consistent_read: Some(consistent_read),
        }
        .execute(table)
        .await
    }

    #[inline]
    pub(crate) fn transact(self) -> GetTransact {
        GetTransact { inner: self }
    }
}

#[derive(Debug, Clone)]
#[must_use]
struct GetOne {
    inner: Get,
    consistent_read: Option<bool>,
}

impl GetOne {
    async fn execute<T: Table>(self, table: &T) -> Result<GetItemOutput, SdkError<GetItemError>> {
        let (projection_expression, projection_names) = if let Some(e) = self.inner.projection {
            (
                Some(e.expression.to_owned()),
                e.names
                    .iter()
                    .map(|(l, r)| (l.to_string(), r.to_string()))
                    .collect::<HashMap<_, _>>(),
            )
        } else {
            (None, Default::default())
        };

        let span = tracing::info_span!(
            "DynamoDB.GetItem",
            span.kind = "client",
            db.system = "dynamodb",
            db.operation = "GetItem",
            db.name = table.table_name(),
            aws.dynamodb.key = ?self.inner.key,
            aws.dynamodb.projection = projection_expression,
            aws.dynamodb.expression_attribute_names = ?projection_names,
            aws.dynamodb.consistent_read = self.consistent_read,
            aws.dynamodb.consumed_read_capacity = field::Empty,
        );

        let result = table
            .client()
            .get_item()
            .set_key((!self.inner.key.is_empty()).then_some(self.inner.key))
            .set_projection_expression(projection_expression)
            .set_expression_attribute_names(
                (!projection_names.is_empty()).then_some(projection_names),
            )
            .set_consistent_read(self.consistent_read)
            .table_name(table.table_name())
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .send()
            .instrument(span.clone())
            .await;

        if let Ok(output) = &result {
            record_consumed_read_capacity(&span, output.consumed_capacity.as_ref());
        }

        result
    }
}

/// A get operation for use in a transaction
#[derive(Debug, Clone)]
#[must_use]
pub struct GetTransact {
    inner: Get,
}

impl GetTransact {
    /// Builds a get operation for inclusion in a transaction
    pub fn build<T: Table>(self, table: &T) -> aws_sdk_dynamodb::types::Get {
        let (projection_expression, projection_names) = if let Some(e) = self.inner.projection {
            (
                Some(e.expression.to_owned()),
                e.names
                    .iter()
                    .map(|(l, r)| (l.to_string(), r.to_string()))
                    .collect::<HashMap<_, _>>(),
            )
        } else {
            (None, Default::default())
        };

        aws_sdk_dynamodb::types::Get::builder()
            .set_key((!self.inner.key.is_empty()).then_some(self.inner.key))
            .set_projection_expression(projection_expression)
            .set_expression_attribute_names(
                (!projection_names.is_empty()).then_some(projection_names),
            )
            .table_name(table.table_name())
            .build()
            .expect("key and table name are always provided")
    }
}

/// A builder for put item operations
#[derive(Debug, Default, Clone)]
#[must_use]
pub struct Put {
    item: Item,
}

impl Put {
    /// Prepare a put item operation
    #[inline]
    pub fn new(item: Item) -> Self {
        Self { item }
    }

    /// Apply a typed conditional expression to the operation
    ///
    /// If the condition evaluates to false, then the operation will fail, but
    /// any relevant write capacity units will still be consumed.
    #[inline]
    pub fn condition(self, condition: expr::Condition) -> ConditionalPut {
        ConditionalPut {
            item: self.item,
            condition: Some(condition),
        }
    }

    /// Execute a single item put operation against the given table
    ///
    /// This method will not return any old or new values.
    pub async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<PutItemOutput, SdkError<PutItemError>> {
        PutOne {
            inner: ConditionalPut {
                item: self.item,
                condition: None,
            },
            return_value: None,
        }
        .execute(table)
        .await
    }

    /// Execute a single item put operation against the given table
    /// with some returned values
    pub async fn execute_with_return<T: Table>(
        self,
        table: &T,
        return_value: ReturnValue,
    ) -> Result<PutItemOutput, SdkError<PutItemError>> {
        PutOne {
            inner: ConditionalPut {
                item: self.item,
                condition: None,
            },
            return_value: Some(return_value),
        }
        .execute(table)
        .await
    }

    /// Prepare a transactional put operation
    #[inline]
    pub fn transact(self) -> PutTransact {
        PutTransact {
            inner: ConditionalPut {
                item: self.item,
                condition: None,
            },
            return_values_on_condition_check_failure: None,
        }
    }

    /// Prepare a transactional put operation, returning the old values if
    /// the condition check fails
    #[inline]
    pub fn transact_with_return_on_fail(self) -> PutTransact {
        PutTransact {
            inner: ConditionalPut {
                item: self.item,
                condition: None,
            },
            return_values_on_condition_check_failure: Some(
                ReturnValuesOnConditionCheckFailure::AllOld,
            ),
        }
    }
}

/// A put operation that has a condition applied
#[derive(Debug, Clone)]
#[must_use]
pub struct ConditionalPut {
    item: Item,
    condition: Option<expr::Condition>,
}

impl ConditionalPut {
    /// Execute a single item put operation against the given table
    ///
    /// This method will not return any old or new values.
    pub async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<PutItemOutput, SdkError<PutItemError>> {
        PutOne {
            inner: self,
            return_value: None,
        }
        .execute(table)
        .await
    }

    /// Execute a single item put operation against the given table
    /// with some returned values
    pub async fn execute_with_return<T: Table>(
        self,
        table: &T,
        return_value: ReturnValue,
    ) -> Result<PutItemOutput, SdkError<PutItemError>> {
        PutOne {
            inner: self,
            return_value: Some(return_value),
        }
        .execute(table)
        .await
    }

    /// Prepare a transactional put operation
    #[inline]
    pub fn transact(self) -> PutTransact {
        PutTransact {
            inner: self,
            return_values_on_condition_check_failure: None,
        }
    }

    /// Prepare a transactional put operation, returning the old values if
    /// the condition check fails
    #[inline]
    pub fn transact_with_return_on_fail(self) -> PutTransact {
        PutTransact {
            inner: self,
            return_values_on_condition_check_failure: Some(
                ReturnValuesOnConditionCheckFailure::AllOld,
            ),
        }
    }
}

#[derive(Debug, Clone)]
#[must_use]
struct PutOne {
    inner: ConditionalPut,
    return_value: Option<ReturnValue>,
}

impl PutOne {
    async fn execute<T: Table>(self, table: &T) -> Result<PutItemOutput, SdkError<PutItemError>> {
        let span = tracing::info_span!(
            "DynamoDB.PutItem",
            span.kind = "client",
            db.system = "dynamodb",
            db.operation = "PutItem",
            db.name = table.table_name(),
            aws.dynamodb.conditional_expression = field::Empty,
            aws.dynamodb.expression_attribute_names = field::Empty,
            aws.dynamodb.expression_attribute_values = field::Empty,
            aws.dynamodb.consumed_write_capacity = field::Empty,
        );

        let mut query = table
            .client()
            .put_item()
            .set_item(Some(self.inner.item))
            .set_return_values(self.return_value)
            .table_name(table.table_name())
            .return_consumed_capacity(ReturnConsumedCapacity::Total);

        if let Some(condition) = self.inner.condition {
            span.record("aws.dynamodb.conditional_expression", &condition.expression);
            let names = if !condition.names.is_empty() {
                let names: HashMap<_, _> = condition.names.into_iter().collect();
                span.record(
                    "aws.dynamodb.expression_attribute_names",
                    field::debug(&names),
                );
                Some(names)
            } else {
                None
            };

            let values = if !condition.values.is_empty() || !condition.sensitive_values.is_empty() {
                let mut values: Item = condition.values.into_iter().collect();
                span.record(
                    "aws.dynamodb.expression_attribute_values",
                    field::debug(&values),
                );

                values.extend(condition.sensitive_values);

                Some(values)
            } else {
                None
            };

            query = query
                .set_condition_expression(Some(condition.expression))
                .set_expression_attribute_names(names)
                .set_expression_attribute_values(values)
        }

        let result = query.send().instrument(span.clone()).await;

        if let Ok(output) = &result {
            record_consumed_write_capacity(&span, output.consumed_capacity.as_ref());
        }

        result
    }
}

/// A put item request for inclusion in a transaction
#[derive(Debug, Clone)]
#[must_use]
pub struct PutTransact {
    inner: ConditionalPut,
    return_values_on_condition_check_failure: Option<ReturnValuesOnConditionCheckFailure>,
}

impl PutTransact {
    /// Builds the put operation targeting a specific table
    pub fn build<T: Table>(self, table: &T) -> aws_sdk_dynamodb::types::Put {
        let mut builder = aws_sdk_dynamodb::types::Put::builder()
            .set_item((!self.inner.item.is_empty()).then_some(self.inner.item))
            .set_table_name(Some(table.table_name().into()))
            .set_return_values_on_condition_check_failure(
                self.return_values_on_condition_check_failure,
            );

        if let Some(condition) = self.inner.condition {
            let names =
                (!condition.names.is_empty()).then(|| condition.names.into_iter().collect());
            let values = (!condition.values.is_empty() || !condition.sensitive_values.is_empty())
                .then(|| {
                    condition
                        .values
                        .into_iter()
                        .chain(condition.sensitive_values)
                        .collect()
                });

            builder = builder
                .set_condition_expression(Some(condition.expression))
                .set_expression_attribute_names(names)
                .set_expression_attribute_values(values)
        }

        builder
            .build()
            .expect("item and table name are always provided")
    }
}

/// A builder for update item operations without an update expression
#[derive(Debug, Clone)]
#[must_use]
pub struct Update {
    key: Item,
}

impl Update {
    /// Prepare a new update item operation
    #[inline]
    pub fn new(key: Item) -> Self {
        Self { key }
    }

    /// The typed update expression to be evaluated
    ///
    /// Example:
    /// ```
    /// use modyne::{EntityDef, EntityExt, IntoUpdate};
    ///
    /// struct MyStructKey {
    ///     id: String
    /// }
    ///
    ///#[derive(EntityDef)]
    /// struct MyStruct {
    ///     id: String,
    ///     field_1: u32,
    ///     field_2: u32
    /// }
    ///
    /// #[derive(IntoUpdate)]
    /// struct MyStructUpdate {
    ///     field_1: Option<u32>,
    ///     field_2: Option<u32>
    /// }
    ///
    /// let update = MyStructUpdate {
    ///     field_1: Some(20)
    ///     field_2: None
    /// }
    /// MyStruct::update(MyStructKey{ id: "Test"}).expression(update)
    /// ```
    /// The above is equivalent to the following manual definition:
    /// 
    /// ```
    /// use modyne::expr::Update;
    /// 
    /// let mut expr = Update::new("");
    ///
    /// if let Some(field_1) = update.field_1 {
    ///     expr = expr.add_expression("SET #field_1 = :field_1");
    ///     expr.name("#field_1", "field_1");
    ///     expr.value(":field_1", field_1);
    /// }
    ///
    /// if let Some(field_2) = update.field_2 {
    ///     expr = expr.add_expression("SET #field_2 = :field_2");
    ///     expr.name("#field_2", "field_2");
    ///     expr.value(":field_2", field_2);
    /// }
    /// 
    /// MyStruct::update(MyStructKey{ id: "Test"}).expression(expr)
    /// ```
    #[inline]
    pub fn expression(self, update: impl Into<expr::Update>) -> UpdateWithExpr {
        UpdateWithExpr {
            key: self.key,
            update: update.into(),
        }
    }
}

/// A builder for update item operations
#[derive(Debug, Clone)]
#[must_use]
pub struct UpdateWithExpr {
    key: Item,
    update: expr::Update,
}

impl UpdateWithExpr {
    /// Apply a typed conditional expression to the operation
    ///
    /// If the condition evaluates to false, then the operation will fail, but
    /// any relevant write capacity units will still be consumed.
    #[inline]
    pub fn condition(self, condition: expr::Condition) -> ConditionalUpdate {
        ConditionalUpdate {
            key: self.key,
            update: self.update,
            condition: Some(condition),
        }
    }

    /// Execute a single item update operation against the given table
    ///
    /// This method will not return any old or new values.
    pub async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<UpdateItemOutput, SdkError<UpdateItemError>> {
        UpdateOne {
            inner: ConditionalUpdate {
                key: self.key,
                update: self.update,
                condition: None,
            },
            return_value: None,
        }
        .execute(table)
        .await
    }

    /// Execute a single item update operation against the given table,
    /// returning the old and/or new values
    pub async fn execute_with_return<T: Table>(
        self,
        table: &T,
        return_value: ReturnValue,
    ) -> Result<UpdateItemOutput, SdkError<UpdateItemError>> {
        UpdateOne {
            inner: ConditionalUpdate {
                key: self.key,
                update: self.update,
                condition: None,
            },
            return_value: Some(return_value),
        }
        .execute(table)
        .await
    }

    /// Prepare a transactional update operation
    #[inline]
    pub fn transact(self) -> UpdateTransact {
        UpdateTransact {
            inner: ConditionalUpdate {
                key: self.key,
                update: self.update,
                condition: None,
            },
            return_values_on_condition_check_failure: None,
        }
    }

    /// Prepare a transactional update operation, returning the old values if
    /// the condition check fails
    #[inline]
    pub fn transact_with_return_on_fail(self) -> UpdateTransact {
        UpdateTransact {
            inner: ConditionalUpdate {
                key: self.key,
                update: self.update,
                condition: None,
            },
            return_values_on_condition_check_failure: Some(
                ReturnValuesOnConditionCheckFailure::AllOld,
            ),
        }
    }
}

/// A conditional update item operation
#[derive(Debug, Clone)]
#[must_use]
pub struct ConditionalUpdate {
    key: Item,
    update: expr::Update,
    condition: Option<expr::Condition>,
}

impl ConditionalUpdate {
    /// Execute a single item update operation against the given table
    ///
    /// This method will not return any old or new values.
    pub async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<UpdateItemOutput, SdkError<UpdateItemError>> {
        UpdateOne {
            inner: self,
            return_value: None,
        }
        .execute(table)
        .await
    }

    /// Execute a single item update operation against the given table,
    /// returning the old and/or new values
    pub async fn execute_with_return<T: Table>(
        self,
        table: &T,
        return_value: ReturnValue,
    ) -> Result<UpdateItemOutput, SdkError<UpdateItemError>> {
        UpdateOne {
            inner: self,
            return_value: Some(return_value),
        }
        .execute(table)
        .await
    }

    /// Prepare a transactional update operation
    #[inline]
    pub fn transact(self) -> UpdateTransact {
        UpdateTransact {
            inner: self,
            return_values_on_condition_check_failure: None,
        }
    }

    /// Prepare a transactional update operation, returning the old values if
    /// the condition check fails
    #[inline]
    pub fn transact_with_return_on_fail(self) -> UpdateTransact {
        UpdateTransact {
            inner: self,
            return_values_on_condition_check_failure: Some(
                ReturnValuesOnConditionCheckFailure::AllOld,
            ),
        }
    }
}

#[derive(Debug, Clone)]
#[must_use]
struct UpdateOne {
    inner: ConditionalUpdate,
    return_value: Option<ReturnValue>,
}

impl UpdateOne {
    async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<UpdateItemOutput, SdkError<UpdateItemError>> {
        let span = tracing::info_span!(
            "DynamoDB.UpdateItem",
            span.kind = "client",
            db.system = "dynamodb",
            db.operation = "UpdateItem",
            db.name = table.table_name(),
            aws.dynamodb.key = ?self.inner.key,
            aws.dynamodb.update_expression = self.inner.update.expression,
            aws.dynamodb.conditional_expression = field::Empty,
            aws.dynamodb.expression_attribute_names = field::Empty,
            aws.dynamodb.expression_attribute_values = field::Empty,
            aws.dynamodb.consumed_write_capacity = field::Empty,
        );

        let mut query = table
            .client()
            .update_item()
            .set_key(Some(self.inner.key))
            .set_update_expression(Some(self.inner.update.expression))
            .set_return_values(self.return_value)
            .set_table_name(Some(table.table_name().into()))
            .return_consumed_capacity(ReturnConsumedCapacity::Total);

        let (cnd_names, cnd_values, cnd_sensitive_values) =
            if let Some(condition) = self.inner.condition {
                span.record("aws.dynamodb.conditional_expression", &condition.expression);
                query = query.set_condition_expression(Some(condition.expression));
                (
                    condition.names,
                    condition.values,
                    condition.sensitive_values,
                )
            } else {
                Default::default()
            };

        let needs_names = !cnd_names.is_empty() || !self.inner.update.names.is_empty();
        let names = needs_names.then(|| {
            cnd_names
                .into_iter()
                .chain(self.inner.update.names)
                .collect()
        });

        span.record(
            "aws.dynamodb.expression_attribute_names",
            field::debug(&names),
        );

        let needs_values = !cnd_values.is_empty()
            || !cnd_sensitive_values.is_empty()
            || !self.inner.update.values.is_empty()
            || !self.inner.update.sensitive_values.is_empty();

        let values = if needs_values {
            let mut vals = HashMap::with_capacity(
                cnd_values.len()
                    + cnd_sensitive_values.len()
                    + self.inner.update.values.len()
                    + self.inner.update.sensitive_values.len(),
            );
            vals.extend(cnd_values);
            vals.extend(self.inner.update.values);

            span.record(
                "aws.dynamodb.expression_attribute_values",
                field::debug(&vals),
            );

            vals.extend(cnd_sensitive_values);
            vals.extend(self.inner.update.sensitive_values);

            Some(vals)
        } else {
            None
        };

        query = query
            .set_expression_attribute_names(names)
            .set_expression_attribute_values(values);

        let result = query.send().instrument(span.clone()).await;

        if let Ok(output) = &result {
            record_consumed_write_capacity(&span, output.consumed_capacity.as_ref());
        }

        result
    }
}

/// A transactional update operation
#[derive(Debug, Clone)]
#[must_use]
pub struct UpdateTransact {
    inner: ConditionalUpdate,
    return_values_on_condition_check_failure: Option<ReturnValuesOnConditionCheckFailure>,
}

impl UpdateTransact {
    /// Narrow the update operation to a specific table
    pub fn build<T: Table>(self, table: &T) -> aws_sdk_dynamodb::types::Update {
        let mut builder = aws_sdk_dynamodb::types::Update::builder()
            .set_key((!self.inner.key.is_empty()).then_some(self.inner.key))
            .set_table_name(Some(table.table_name().into()))
            .set_return_values_on_condition_check_failure(
                self.return_values_on_condition_check_failure,
            )
            .set_update_expression(Some(self.inner.update.expression));

        if let Some(condition) = self.inner.condition {
            let needs_names = !condition.names.is_empty() || !self.inner.update.names.is_empty();
            let names = needs_names.then(|| {
                condition
                    .names
                    .into_iter()
                    .chain(self.inner.update.names)
                    .collect()
            });
            let needs_values = !condition.values.is_empty()
                || !condition.sensitive_values.is_empty()
                || !self.inner.update.values.is_empty()
                || !self.inner.update.sensitive_values.is_empty();
            let values = needs_values.then(|| {
                condition
                    .values
                    .into_iter()
                    .chain(self.inner.update.values)
                    .chain(condition.sensitive_values)
                    .chain(self.inner.update.sensitive_values)
                    .collect()
            });

            builder = builder
                .set_condition_expression(Some(condition.expression))
                .set_expression_attribute_names(names)
                .set_expression_attribute_values(values)
        } else {
            let needs_names = !self.inner.update.names.is_empty();
            let names = needs_names.then(|| self.inner.update.names.into_iter().collect());
            let needs_values = !self.inner.update.values.is_empty()
                || !self.inner.update.sensitive_values.is_empty();
            let values = needs_values.then(|| {
                self.inner
                    .update
                    .values
                    .into_iter()
                    .chain(self.inner.update.sensitive_values)
                    .collect()
            });

            builder = builder
                .set_expression_attribute_names(names)
                .set_expression_attribute_values(values)
        }

        builder
            .build()
            .expect("key, update expression, and table name are always provided")
    }
}

/// A builder for delete item operations
#[derive(Debug, Clone)]
#[must_use]
pub struct Delete {
    key: Item,
}

impl Delete {
    /// Prepare a new delete operation
    #[inline]
    pub fn new(key: Item) -> Self {
        Self { key }
    }

    /// Apply a typed conditional expression to the operation
    ///
    /// If the condition evaluates to false, then the operation will fail, but
    /// any relevant write capacity units will still be consumed.
    #[inline]
    pub fn condition(self, condition: expr::Condition) -> ConditionalDelete {
        ConditionalDelete {
            key: self.key,
            condition: Some(condition),
        }
    }

    /// Execute a single item delete operation against the given table
    ///
    /// This method will not return the old values.
    pub async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<DeleteItemOutput, SdkError<DeleteItemError>> {
        DeleteOne {
            inner: ConditionalDelete {
                key: self.key,
                condition: None,
            },
            return_value: None,
        }
        .execute(table)
        .await
    }

    /// Execute a single item delete operation against the given table,
    /// returning the old values
    pub async fn execute_with_return<T: Table>(
        self,
        table: &T,
    ) -> Result<DeleteItemOutput, SdkError<DeleteItemError>> {
        DeleteOne {
            inner: ConditionalDelete {
                key: self.key,
                condition: None,
            },
            return_value: Some(ReturnValue::AllOld),
        }
        .execute(table)
        .await
    }

    /// Prepare a transactional delete operation
    #[inline]
    pub fn transact(self) -> DeleteTransact {
        DeleteTransact {
            inner: ConditionalDelete {
                key: self.key,
                condition: None,
            },
            return_values_on_condition_check_failure: None,
        }
    }

    /// Prepare a transactional delete operation, returning the old values if
    /// the condition check fails
    #[inline]
    pub fn transact_with_return_on_fail(self) -> DeleteTransact {
        DeleteTransact {
            inner: ConditionalDelete {
                key: self.key,
                condition: None,
            },
            return_values_on_condition_check_failure: Some(
                ReturnValuesOnConditionCheckFailure::AllOld,
            ),
        }
    }
}

/// A delete operation that has a condition applied
#[derive(Debug, Clone)]
#[must_use]
pub struct ConditionalDelete {
    condition: Option<expr::Condition>,
    key: Item,
}

impl ConditionalDelete {
    /// Execute a single item delete operation against the given table
    ///
    /// This method will not return the old values.
    pub async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<DeleteItemOutput, SdkError<DeleteItemError>> {
        DeleteOne {
            inner: self,
            return_value: None,
        }
        .execute(table)
        .await
    }

    /// Execute a single item delete operation against the given table,
    /// returning the old values
    pub async fn execute_with_return<T: Table>(
        self,
        table: &T,
    ) -> Result<DeleteItemOutput, SdkError<DeleteItemError>> {
        DeleteOne {
            inner: self,
            return_value: Some(ReturnValue::AllOld),
        }
        .execute(table)
        .await
    }

    /// Prepare a transactional delete operation
    #[inline]
    pub fn transact(self) -> DeleteTransact {
        DeleteTransact {
            inner: self,
            return_values_on_condition_check_failure: None,
        }
    }

    /// Prepare a transactional delete operation, returning the old values if
    /// the condition check fails
    #[inline]
    pub fn transact_with_return_on_fail(self) -> DeleteTransact {
        DeleteTransact {
            inner: self,
            return_values_on_condition_check_failure: Some(
                ReturnValuesOnConditionCheckFailure::AllOld,
            ),
        }
    }
}

#[derive(Debug, Clone)]
#[must_use]
struct DeleteOne {
    inner: ConditionalDelete,
    return_value: Option<ReturnValue>,
}

impl DeleteOne {
    async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<DeleteItemOutput, SdkError<DeleteItemError>> {
        let span = tracing::info_span!(
            "DynamoDB.DeleteItem",
            span.kind = "client",
            db.system = "dynamodb",
            db.operation = "DeleteItem",
            db.name = table.table_name(),
            aws.dynamodb.key = ?self.inner.key,
            aws.dynamodb.conditional_expression = field::Empty,
            aws.dynamodb.expression_attribute_names = field::Empty,
            aws.dynamodb.expression_attribute_values = field::Empty,
            aws.dynamodb.consumed_write_capacity = field::Empty,
        );

        let mut query = table
            .client()
            .delete_item()
            .set_key(Some(self.inner.key))
            .set_return_values(self.return_value)
            .table_name(table.table_name())
            .return_consumed_capacity(ReturnConsumedCapacity::Total);

        if let Some(condition) = self.inner.condition {
            span.record("aws.dynamodb.conditional_expression", &condition.expression);
            let names = if !condition.names.is_empty() {
                let names: HashMap<_, _> = condition.names.into_iter().collect();
                span.record(
                    "aws.dynamodb.expression_attribute_names",
                    field::debug(&names),
                );
                Some(names)
            } else {
                None
            };

            let values = if !condition.values.is_empty() || !condition.sensitive_values.is_empty() {
                let mut values: Item = condition.values.into_iter().collect();
                span.record(
                    "aws.dynamodb.expression_attribute_values",
                    field::debug(&values),
                );

                values.extend(condition.sensitive_values);

                Some(values)
            } else {
                None
            };

            query = query
                .set_condition_expression(Some(condition.expression))
                .set_expression_attribute_names(names)
                .set_expression_attribute_values(values)
        }

        let result = query.send().instrument(span.clone()).await;

        if let Ok(output) = &result {
            record_consumed_write_capacity(&span, output.consumed_capacity.as_ref());
        }

        result
    }
}

/// A transactional delete operation
#[derive(Debug, Clone)]
#[must_use]
pub struct DeleteTransact {
    inner: ConditionalDelete,
    return_values_on_condition_check_failure: Option<ReturnValuesOnConditionCheckFailure>,
}

impl DeleteTransact {
    /// Narrow the delete operation to a specific table
    pub fn build<T: Table>(self, table: &T) -> aws_sdk_dynamodb::types::Delete {
        let mut builder = aws_sdk_dynamodb::types::Delete::builder()
            .set_key((!self.inner.key.is_empty()).then_some(self.inner.key))
            .set_table_name(Some(table.table_name().into()))
            .set_return_values_on_condition_check_failure(
                self.return_values_on_condition_check_failure,
            );

        if let Some(condition) = self.inner.condition {
            let names =
                (!condition.names.is_empty()).then(|| condition.names.into_iter().collect());
            let values = (!condition.values.is_empty() || !condition.sensitive_values.is_empty())
                .then(|| {
                    condition
                        .values
                        .into_iter()
                        .chain(condition.sensitive_values)
                        .collect()
                });

            builder = builder
                .set_condition_expression(Some(condition.expression))
                .set_expression_attribute_names(names)
                .set_expression_attribute_values(values)
        }

        builder
            .build()
            .expect("key and table name are always provided")
    }
}

/// A builder for transactional condition check operations
#[derive(Debug, Clone)]
#[must_use]
pub struct ConditionCheck {
    condition: expr::Condition,
    key: Item,
}

impl ConditionCheck {
    /// Prepare a new condition check operation
    #[inline]
    pub fn new(key: Item, condition: expr::Condition) -> Self {
        Self { key, condition }
    }

    /// Prepare a transactional condition check operation
    #[inline]
    pub fn transact(self) -> ConditionCheckTransact {
        ConditionCheckTransact {
            inner: self,
            return_values_on_condition_check_failure: None,
        }
    }

    /// Prepare a transactional condition check operation, returning the old
    /// values if the condition check fails
    #[inline]
    pub fn transact_with_return_on_fail(self) -> ConditionCheckTransact {
        ConditionCheckTransact {
            inner: self,
            return_values_on_condition_check_failure: Some(
                ReturnValuesOnConditionCheckFailure::AllOld,
            ),
        }
    }
}

/// A transactional condition check
#[derive(Clone, Debug)]
#[must_use]
pub struct ConditionCheckTransact {
    inner: ConditionCheck,
    return_values_on_condition_check_failure: Option<ReturnValuesOnConditionCheckFailure>,
}

impl ConditionCheckTransact {
    /// Narrow the condition check operation to a specific table
    pub fn build<T: Table>(self, table: &T) -> aws_sdk_dynamodb::types::ConditionCheck {
        let is_empty = self.inner.condition.values.is_empty()
            && self.inner.condition.sensitive_values.is_empty();

        let chain = self
            .inner
            .condition
            .values
            .into_iter()
            .chain(self.inner.condition.sensitive_values);

        aws_sdk_dynamodb::types::ConditionCheck::builder()
            .set_condition_expression(Some(self.inner.condition.expression))
            .set_expression_attribute_names(
                (!self.inner.condition.names.is_empty())
                    .then(|| self.inner.condition.names.into_iter().collect()),
            )
            .set_expression_attribute_values((!is_empty).then(|| chain.collect()))
            .set_key((!self.inner.key.is_empty()).then_some(self.inner.key))
            .set_return_values_on_condition_check_failure(
                self.return_values_on_condition_check_failure,
            )
            .set_table_name(Some(table.table_name().into()))
            .build()
            .expect("key, condition expression, and table name are always provided")
    }
}

/// A transactional write operation
#[derive(Debug, Clone)]
#[must_use]
pub enum TransactWriteItem {
    /// A transactional put
    PutItem(PutTransact),
    /// A transactional update
    UpdateItem(UpdateTransact),
    /// A transactional delete
    DeleteItem(DeleteTransact),
    /// A transactional condition check without modification
    ConditionCheck(ConditionCheckTransact),
}

impl TransactWriteItem {
    fn into_batch<T: Table>(self, table: &T) -> aws_sdk_dynamodb::types::TransactWriteItem {
        match self {
            TransactWriteItem::PutItem(op) => aws_sdk_dynamodb::types::TransactWriteItem::builder()
                .put(op.build(table))
                .build(),
            TransactWriteItem::UpdateItem(op) => {
                aws_sdk_dynamodb::types::TransactWriteItem::builder()
                    .update(op.build(table))
                    .build()
            }
            TransactWriteItem::DeleteItem(op) => {
                aws_sdk_dynamodb::types::TransactWriteItem::builder()
                    .delete(op.build(table))
                    .build()
            }
            TransactWriteItem::ConditionCheck(op) => {
                aws_sdk_dynamodb::types::TransactWriteItem::builder()
                    .condition_check(op.build(table))
                    .build()
            }
        }
    }
}

impl From<PutTransact> for TransactWriteItem {
    #[inline]
    fn from(op: PutTransact) -> Self {
        TransactWriteItem::PutItem(op)
    }
}

impl From<UpdateTransact> for TransactWriteItem {
    #[inline]
    fn from(op: UpdateTransact) -> Self {
        TransactWriteItem::UpdateItem(op)
    }
}

impl From<DeleteTransact> for TransactWriteItem {
    #[inline]
    fn from(op: DeleteTransact) -> Self {
        TransactWriteItem::DeleteItem(op)
    }
}

impl From<ConditionCheckTransact> for TransactWriteItem {
    #[inline]
    fn from(op: ConditionCheckTransact) -> Self {
        TransactWriteItem::ConditionCheck(op)
    }
}

impl From<Put> for TransactWriteItem {
    #[inline]
    fn from(op: Put) -> Self {
        TransactWriteItem::PutItem(op.transact())
    }
}

impl From<ConditionalPut> for TransactWriteItem {
    #[inline]
    fn from(op: ConditionalPut) -> Self {
        TransactWriteItem::PutItem(op.transact())
    }
}

impl From<UpdateWithExpr> for TransactWriteItem {
    #[inline]
    fn from(op: UpdateWithExpr) -> Self {
        TransactWriteItem::UpdateItem(op.transact())
    }
}

impl From<ConditionalUpdate> for TransactWriteItem {
    #[inline]
    fn from(op: ConditionalUpdate) -> Self {
        TransactWriteItem::UpdateItem(op.transact())
    }
}

impl From<Delete> for TransactWriteItem {
    #[inline]
    fn from(op: Delete) -> Self {
        TransactWriteItem::DeleteItem(op.transact())
    }
}

impl From<ConditionalDelete> for TransactWriteItem {
    #[inline]
    fn from(op: ConditionalDelete) -> Self {
        TransactWriteItem::DeleteItem(op.transact())
    }
}

impl From<ConditionCheck> for TransactWriteItem {
    #[inline]
    fn from(op: ConditionCheck) -> Self {
        TransactWriteItem::ConditionCheck(op.transact())
    }
}

/// A transactional get operation
#[derive(Debug, Default, Clone)]
#[must_use]
pub struct TransactGet {
    operations: Vec<GetTransact>,
}

impl TransactGet {
    /// Prepare a new transactional get operation
    #[inline]
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Attach a get operation to the transaction
    #[inline]
    pub fn operation(mut self, op: Get) -> Self {
        self.operations.push(op.transact());
        self
    }

    /// Execute the transaction
    pub async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<TransactGetItemsOutput, SdkError<TransactGetItemsError>> {
        let span = tracing::info_span!(
            "DynamoDB.TransactGetItems",
            span.kind = "client",
            db.system = "dynamodb",
            db.operation = "TransactGetItems",
            db.name = table.table_name(),
            aws.dynamodb.table_names = ?[&table.table_name()],
            aws.dynamodb.table_count = 1,
            aws.dynamodb.batch_operations = self.operations.len(),
            aws.dynamodb.consumed_read_capacity = field::Empty,
        );

        let items = if self.operations.is_empty() {
            None
        } else {
            Some(
                self.operations
                    .into_iter()
                    .map(move |i| {
                        aws_sdk_dynamodb::types::TransactGetItem::builder()
                            .get(i.build(table))
                            .build()
                    })
                    .collect(),
            )
        };

        let result = table
            .client()
            .transact_get_items()
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .set_transact_items(items)
            .send()
            .instrument(span.clone())
            .await;

        if let Ok(output) = &result {
            let capacity = output.consumed_capacity().iter().fold(
                ConsumedCapacity::builder().build(),
                |mut acc, next| {
                    acc.capacity_units = merge_values(acc.capacity_units, next.capacity_units);
                    acc.read_capacity_units =
                        merge_values(acc.read_capacity_units, next.read_capacity_units);
                    acc
                },
            );
            record_consumed_read_capacity(&span, Some(&capacity));
        }

        result
    }
}

/// A transactional write operation
#[derive(Debug, Default, Clone)]
#[must_use]
pub struct TransactWrite {
    client_request_token: Option<String>,
    operations: Vec<TransactWriteItem>,
}

impl TransactWrite {
    /// Prepare a new transactional write operation
    #[inline]
    pub fn new() -> Self {
        Self {
            client_request_token: None,
            operations: Vec::new(),
        }
    }

    /// Apply an idempotency token to the write request
    #[inline]
    pub fn client_request_token(mut self, client_request_token: impl Into<String>) -> Self {
        self.client_request_token = Some(client_request_token.into());
        self
    }

    /// Attach a write operation to the transaction
    #[inline]
    pub fn operation(mut self, op: impl Into<TransactWriteItem>) -> Self {
        self.operations.push(op.into());
        self
    }

    /// Execute the write transaction
    pub async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<TransactWriteItemsOutput, SdkError<TransactWriteItemsError>> {
        let span = tracing::info_span!(
            "DynamoDB.TransactWriteItems",
            span.kind = "client",
            db.system = "dynamodb",
            db.operation = "TransactWriteItems",
            db.name = table.table_name(),
            aws.dynamodb.table_names = ?[&table.table_name()],
            aws.dynamodb.table_count = 1,
            aws.dynamodb.batch_operations = self.operations.len(),
            aws.dynamodb.consumed_write_capacity = field::Empty,
        );

        let items = if self.operations.is_empty() {
            None
        } else {
            Some(
                self.operations
                    .into_iter()
                    .map(move |i| i.into_batch(table))
                    .collect(),
            )
        };

        let result = table
            .client()
            .transact_write_items()
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .set_transact_items(items)
            .set_client_request_token(self.client_request_token)
            .send()
            .instrument(span.clone())
            .await;

        if let Ok(output) = &result {
            let capacity = output.consumed_capacity().iter().fold(
                ConsumedCapacity::builder().build(),
                |mut acc, next| {
                    acc.capacity_units = merge_values(acc.capacity_units, next.capacity_units);
                    acc.write_capacity_units =
                        merge_values(acc.write_capacity_units, next.write_capacity_units);
                    acc
                },
            );
            record_consumed_write_capacity(&span, Some(&capacity));
        }

        result
    }
}

/// A transactional write operation
#[derive(Debug, Clone)]
#[must_use]
pub enum BatchWriteItem {
    /// A batch put
    PutItem(Put),
    /// A transactional delete
    DeleteItem(Delete),
}

impl BatchWriteItem {
    #[inline]
    fn into_batch(self) -> aws_sdk_dynamodb::types::WriteRequest {
        match self {
            Self::PutItem(op) => aws_sdk_dynamodb::types::WriteRequest::builder()
                .put_request(
                    aws_sdk_dynamodb::types::PutRequest::builder()
                        .set_item(Some(op.item))
                        .build()
                        .expect("item is always provided"),
                )
                .build(),
            Self::DeleteItem(op) => aws_sdk_dynamodb::types::WriteRequest::builder()
                .delete_request(
                    aws_sdk_dynamodb::types::DeleteRequest::builder()
                        .set_key(Some(op.key))
                        .build()
                        .expect("key is always provided"),
                )
                .build(),
        }
    }
}

impl From<Put> for BatchWriteItem {
    #[inline]
    fn from(op: Put) -> Self {
        Self::PutItem(op)
    }
}

impl From<Delete> for BatchWriteItem {
    #[inline]
    fn from(op: Delete) -> Self {
        Self::DeleteItem(op)
    }
}
/// A batch get operation
#[derive(Debug, Default, Clone)]
#[must_use]
pub struct BatchGet {
    operations: Vec<Get>,
}

impl BatchGet {
    /// Prepare a new batch get operation
    #[inline]
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Attach a get operation to the batch
    #[inline]
    pub fn operation(mut self, op: Get) -> Self {
        self.operations.push(op);
        self
    }

    /// Execute the batch
    pub async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<BatchGetItemOutput, SdkError<BatchGetItemError>> {
        let span = tracing::info_span!(
            "DynamoDB.BatchGetItem",
            span.kind = "client",
            db.system = "dynamodb",
            db.operation = "BatchGetItem",
            db.name = table.table_name(),
            aws.dynamodb.table_names = ?[&table.table_name()],
            aws.dynamodb.table_count = 1,
            aws.dynamodb.batch_operations = self.operations.len(),
            aws.dynamodb.consumed_read_capacity = field::Empty,
        );

        let items = if self.operations.is_empty() {
            None
        } else {
            let mut kattr = KeysAndAttributes::builder();
            for item in self.operations {
                kattr = kattr.keys(item.key);
            }
            let tables = [(
                table.table_name().to_owned(),
                kattr.build().expect("keys is always provided"),
            )]
            .into_iter()
            .collect();
            Some(tables)
        };

        let result = table
            .client()
            .batch_get_item()
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .set_request_items(items)
            .send()
            .instrument(span.clone())
            .await;

        if let Ok(output) = &result {
            let capacity = output.consumed_capacity().iter().fold(
                ConsumedCapacity::builder().build(),
                |mut acc, next| {
                    acc.capacity_units = merge_values(acc.capacity_units, next.capacity_units);
                    acc.read_capacity_units =
                        merge_values(acc.read_capacity_units, next.read_capacity_units);
                    acc
                },
            );
            record_consumed_read_capacity(&span, Some(&capacity));
        }

        result
    }
}

/// A batch write operation
#[derive(Debug, Default, Clone)]
#[must_use]
pub struct BatchWrite {
    operations: Vec<BatchWriteItem>,
}

impl BatchWrite {
    /// Prepare a new batch write operation
    #[inline]
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Attach a write operation to the batch
    #[inline]
    pub fn operation(mut self, op: impl Into<BatchWriteItem>) -> Self {
        self.operations.push(op.into());
        self
    }

    /// Execute the write batch
    pub async fn execute<T: Table>(
        self,
        table: &T,
    ) -> Result<BatchWriteItemOutput, SdkError<BatchWriteItemError>> {
        let span = tracing::info_span!(
            "DynamoDB.BatchWriteItem",
            span.kind = "client",
            db.system = "dynamodb",
            db.operation = "BatchWriteItem",
            db.name = table.table_name(),
            aws.dynamodb.table_names = ?[&table.table_name()],
            aws.dynamodb.table_count = 1,
            aws.dynamodb.batch_operations = self.operations.len(),
            aws.dynamodb.consumed_write_capacity = field::Empty,
        );

        let items = if self.operations.is_empty() {
            None
        } else {
            let reqs = self
                .operations
                .into_iter()
                .map(BatchWriteItem::into_batch)
                .collect();
            let tables = [(table.table_name().to_owned(), reqs)]
                .into_iter()
                .collect();
            Some(tables)
        };

        let result = table
            .client()
            .batch_write_item()
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .set_request_items(items)
            .send()
            .instrument(span.clone())
            .await;

        if let Ok(output) = &result {
            let capacity = output.consumed_capacity().iter().fold(
                ConsumedCapacity::builder().build(),
                |mut acc, next| {
                    acc.capacity_units = merge_values(acc.capacity_units, next.capacity_units);
                    acc.write_capacity_units =
                        merge_values(acc.write_capacity_units, next.write_capacity_units);
                    acc
                },
            );
            record_consumed_write_capacity(&span, Some(&capacity));
        }

        result
    }
}

/// A builder for index query operations
#[must_use]
pub struct Query<K> {
    key_condition: expr::KeyCondition<K>,
    projection: Option<expr::StaticProjection>,
    filter: Option<expr::Filter>,
    limit: Option<i32>,
    select: Option<Select>,
    scan_index_forward: bool,
    consistent_read: bool,
    exclusive_start_key: Option<Item>,
}

impl<K> fmt::Debug for Query<K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Query")
            .field("key_condition", &self.key_condition)
            .field("projection", &self.projection)
            .field("filter", &self.filter)
            .field("limit", &self.limit)
            .field("select", &self.select)
            .field("consistent_read", &self.consistent_read)
            .field("scan_index_forward", &self.scan_index_forward)
            .field("exclusive_start_key", &self.exclusive_start_key)
            .finish()
    }
}

impl<K> Clone for Query<K> {
    fn clone(&self) -> Self {
        Self {
            key_condition: self.key_condition.clone(),
            projection: self.projection,
            filter: self.filter.clone(),
            limit: self.limit,
            select: self.select.clone(),
            consistent_read: self.consistent_read,
            scan_index_forward: self.scan_index_forward,
            exclusive_start_key: self.exclusive_start_key.clone(),
        }
    }
}

impl<K: keys::Key> Query<K> {
    /// Construct a query with the given key condition
    pub fn new(key_condition: expr::KeyCondition<K>) -> Self {
        Self {
            key_condition,
            projection: None,
            filter: None,
            limit: None,
            select: None,
            scan_index_forward: true,
            consistent_read: false,
            exclusive_start_key: None,
        }
    }

    /// Override the group of attributes returned by the query
    pub fn select(mut self, select: Select) -> Self {
        self.select = Some(select);
        self
    }

    /// Set a specific limit on the number of items scanned before returning
    ///
    /// The number of items returned may be less than the number scanned due
    /// to filter expressions.
    pub fn limit(mut self, limit: u32) -> Self {
        if limit > i32::MAX as u32 {
            self.limit = None;
        } else {
            self.limit = Some(limit as i32);
        }
        self
    }

    /// Set a specific limit on the number of items scanned before returning
    ///
    /// The number of items returned may be less than the number scanned due
    /// to filter expressions.
    pub fn set_limit(mut self, limit: Option<u32>) -> Self {
        if let Some(limit) = limit {
            self.limit(limit)
        } else {
            self.limit = None;
            self
        }
    }

    /// Mark the query as requiring consistent reads
    pub fn consistent_read(mut self) -> Self {
        self.consistent_read = true;
        self
    }

    /// Scan the index in the reverse direction
    pub fn scan_index_backward(mut self) -> Self {
        self.scan_index_forward = false;
        self
    }

    /// Set the sort key to start the scan from, for pagination
    pub fn exclusive_start_key(mut self, item: Item) -> Self {
        self.exclusive_start_key = Some(item);
        self
    }

    /// Set the sort key to start the query from, for pagination
    pub fn set_exclusive_start_key(mut self, item: Option<Item>) -> Self {
        self.exclusive_start_key = item;
        self
    }

    /// Override the set of attributes projected into the response
    ///
    /// # Note
    ///
    /// The entire size of an item counts toward RCU consumption, whether or not
    /// all attributes are projected.
    pub fn projection(mut self, projection: expr::StaticProjection) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Apply a filter expression to the scanned items
    ///
    /// # Note
    ///
    /// All items scanned count toward RCU consumption, whether or not they are
    /// returned as a result of the filter.
    pub fn filter(mut self, filter: expr::Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Execute the query operation against the specified table
    pub async fn execute<T: Table>(self, table: &T) -> Result<QueryOutput, SdkError<QueryError>> {
        let (filter_expr, filter_names, filter_values, filter_sensitive_values) = {
            if let Some(f) = self.filter {
                (
                    Some(f.expression),
                    Some(f.names),
                    Some(f.values),
                    Some(f.sensitive_values),
                )
            } else {
                (None, None, None, None)
            }
        };

        let key_condition_expr = self.key_condition.expression();

        let expression_attribute_names = self
            .key_condition
            .names()
            .chain(
                self.projection
                    .map(|f| f.names)
                    .into_iter()
                    .flatten()
                    .copied(),
            )
            .map(|(l, r)| (l.to_string(), r.to_string()))
            .chain(filter_names.into_iter().flatten())
            .collect::<HashMap<String, String>>();

        let mut expression_attribute_values = self
            .key_condition
            .values()
            .map(|(l, r)| (l.to_string(), r))
            .chain(filter_values.into_iter().flatten())
            .collect::<HashMap<String, AttributeValue>>();

        let span = tracing::info_span!(
            "DynamoDB.Query",
            span.kind = "client",
            db.system = "dynamodb",
            db.operation = "Query",
            db.name = table.table_name(),
            aws.dynamodb.index_name = K::DEFINITION.index_name(),
            aws.dynamodb.filter_expression = filter_expr.as_deref(),
            aws.dynamodb.projection = self.projection.map(|p| p.expression),
            aws.dynamodb.key_condition_expression = key_condition_expr,
            aws.dynamodb.exclusive_start_key = self.exclusive_start_key.as_ref().map(tracing::field::debug),
            aws.dynamodb.limit = self.limit,
            aws.dynamodb.select = self.select.as_ref().map(tracing::field::debug),
            aws.dynamodb.scan_forward = self.scan_index_forward,
            aws.dynamodb.consistent_read = self.consistent_read,
            aws.dynamodb.expression_attribute_names = ?expression_attribute_names,
            aws.dynamodb.expression_attribute_values = ?expression_attribute_values,
            aws.dynamodb.consumed_read_capacity = field::Empty,
            aws.dynamodb.scanned_count = field::Empty,
            aws.dynamodb.count = field::Empty,
            aws.dynamodb.has_next_page = field::Empty,
        );

        expression_attribute_values.extend(filter_sensitive_values.into_iter().flatten());

        let result = table
            .client()
            .query()
            .table_name(table.table_name())
            .set_index_name(K::DEFINITION.index_name().map(|i| i.to_string()))
            .set_select(self.select)
            .set_limit(self.limit)
            .set_consistent_read(self.consistent_read.then_some(true))
            .set_scan_index_forward((!self.scan_index_forward).then_some(false))
            .set_exclusive_start_key(self.exclusive_start_key)
            .set_projection_expression(self.projection.map(|p| p.expression.to_string()))
            .set_filter_expression(filter_expr)
            .set_key_condition_expression(Some(key_condition_expr.to_string()))
            .set_expression_attribute_names(
                (!expression_attribute_names.is_empty()).then_some(expression_attribute_names),
            )
            .set_expression_attribute_values(
                (!expression_attribute_values.is_empty()).then_some(expression_attribute_values),
            )
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .send()
            .instrument(span.clone())
            .await;

        if let Ok(output) = &result {
            record_consumed_read_capacity(&span, output.consumed_capacity.as_ref());
            span.record("aws.dynamodb.scanned_count", output.scanned_count());
            span.record("aws.dynamodb.count", output.count());
            span.record(
                "aws.dynamodb.has_next_page",
                output.last_evaluated_key().is_some(),
            );
        }

        result
    }
}

/// The segment of a scan operation to be performed
#[derive(Clone, Copy, Debug)]
pub struct ScanSegment {
    /// The segment of `total_segments`
    pub segment: i32,

    /// Total of all segments
    pub total_segments: i32,
}

/// A builder for scan operations
#[must_use]
pub struct Scan<K> {
    limit: Option<i32>,
    select: Option<Select>,
    consistent_read: bool,
    segment: Option<ScanSegment>,
    exclusive_start_key: Option<Item>,
    projection: Option<expr::StaticProjection>,
    filter: Option<expr::Filter>,
    key_type: PhantomData<fn() -> K>,
}

impl<K> fmt::Debug for Scan<K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Scan")
            .field("key_type", &std::any::type_name::<K>())
            .field("limit", &self.limit)
            .field("select", &self.select)
            .field("consistent_read", &self.consistent_read)
            .field("segment", &self.segment)
            .field("exclusive_start_key", &self.exclusive_start_key)
            .field("projection", &self.projection)
            .field("filter", &self.filter)
            .finish()
    }
}

impl<K> Clone for Scan<K> {
    fn clone(&self) -> Self {
        Self {
            limit: self.limit,
            select: self.select.clone(),
            consistent_read: self.consistent_read,
            segment: self.segment,
            exclusive_start_key: self.exclusive_start_key.clone(),
            projection: self.projection,
            filter: self.filter.clone(),
            key_type: PhantomData,
        }
    }
}

impl<K: keys::Key> Default for Scan<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: keys::Key> Scan<K> {
    /// Prepare a scan operation against the given index
    pub fn new() -> Self {
        Self {
            limit: None,
            select: None,
            consistent_read: false,
            segment: None,
            exclusive_start_key: None,
            projection: None,
            filter: None,
            key_type: PhantomData,
        }
    }

    /// Set the segment assigned to this scan operation
    pub fn segment(mut self, segment: ScanSegment) -> Self {
        self.segment = Some(segment);
        self
    }

    /// Override the group of attributes returned by the scan
    pub fn select(mut self, select: Select) -> Self {
        self.select = Some(select);
        self
    }

    /// Set a specific limit on the number of items scanned before returning
    ///
    /// The number of items returned may be less than the number scanned due
    /// to filter expressions.
    pub fn limit(mut self, limit: u32) -> Self {
        if limit > i32::MAX as u32 {
            self.limit = None;
        } else {
            self.limit = Some(limit as i32);
        }
        self
    }

    /// Set a specific limit on the number of items scanned before returning
    ///
    /// The number of items returned may be less than the number scanned due
    /// to filter expressions.
    pub fn set_limit(mut self, limit: Option<u32>) -> Self {
        if let Some(limit) = limit {
            self.limit(limit)
        } else {
            self.limit = None;
            self
        }
    }

    /// Mark the scan as requiring consistent reads
    pub fn consistent_read(mut self) -> Self {
        self.consistent_read = true;
        self
    }

    /// Set the sort key to start the scan from, for pagination
    pub fn exclusive_start_key(mut self, item: Item) -> Self {
        self.exclusive_start_key = Some(item);
        self
    }

    /// Set the sort key to start the scan from, for pagination
    pub fn set_exclusive_start_key(mut self, item: Option<Item>) -> Self {
        self.exclusive_start_key = item;
        self
    }

    /// Override the set of attributes projected into the response
    ///
    /// # Note
    ///
    /// The entire size of an item counts toward RCU consumption, whether or not
    /// all attributes are projected.
    pub fn projection(mut self, projection: expr::StaticProjection) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Apply a filter expression to the scanned items
    ///
    /// # Note
    ///
    /// All items scanned count toward RCU consumption, whether or not they are
    /// returned as a result of the filter.
    pub fn filter(mut self, filter: expr::Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Execute the scan operation against the specified table
    pub async fn execute<T: Table>(self, table: &T) -> Result<ScanOutput, SdkError<ScanError>> {
        let (filter_expr, filter_names, filter_values, filter_sensitive_values) = {
            if let Some(f) = self.filter {
                (
                    Some(f.expression),
                    Some(f.names),
                    Some(f.values),
                    Some(f.sensitive_values),
                )
            } else {
                (None, None, None, None)
            }
        };

        let expression_attribute_names = self
            .projection
            .map(|f| f.names)
            .into_iter()
            .flatten()
            .copied()
            .map(|(l, r)| (l.to_string(), r.to_string()))
            .chain(filter_names.into_iter().flatten())
            .collect::<HashMap<String, String>>();

        let mut expression_attribute_values: HashMap<_, _> =
            filter_values.unwrap_or_default().into_iter().collect();

        let segment = self.segment.map(|s| s.segment);
        let total_segments = self.segment.map(|s| s.total_segments);

        let span = tracing::info_span!(
            "DynamoDB.Scan",
            span.kind = "client",
            db.system = "dynamodb",
            db.operation = "Scan",
            db.name = table.table_name(),
            aws.dynamodb.index_name = K::DEFINITION.index_name(),
            aws.dynamodb.filter_expression = filter_expr.as_deref(),
            aws.dynamodb.projection = self.projection.map(|p| p.expression),
            aws.dynamodb.exclusive_start_key = self.exclusive_start_key.as_ref().map(tracing::field::debug),
            aws.dynamodb.limit = self.limit,
            aws.dynamodb.select = self.select.as_ref().map(tracing::field::debug),
            aws.dynamodb.consistent_read = self.consistent_read,
            aws.dynamodb.expression_attribute_names = ?expression_attribute_names,
            aws.dynamodb.expression_attribute_values = ?expression_attribute_values,
            aws.dynamodb.segment = segment,
            aws.dynamodb.total_segments = total_segments,
            aws.dynamodb.consumed_read_capacity = field::Empty,
            aws.dynamodb.scanned_count = field::Empty,
            aws.dynamodb.count = field::Empty,
            aws.dynamodb.has_next_page = field::Empty,
        );

        expression_attribute_values.extend(filter_sensitive_values.into_iter().flatten());

        let result = table
            .client()
            .scan()
            .table_name(table.table_name())
            .set_index_name(K::DEFINITION.index_name().map(|i| i.to_string()))
            .set_select(self.select)
            .set_limit(self.limit)
            .set_consistent_read(self.consistent_read.then_some(true))
            .set_segment(segment)
            .set_total_segments(total_segments)
            .set_exclusive_start_key(self.exclusive_start_key)
            .set_projection_expression(self.projection.map(|p| p.expression.to_string()))
            .set_filter_expression(filter_expr)
            .set_expression_attribute_names(
                (!expression_attribute_names.is_empty()).then_some(expression_attribute_names),
            )
            .set_expression_attribute_values(
                (!expression_attribute_values.is_empty()).then_some(expression_attribute_values),
            )
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .send()
            .instrument(span.clone())
            .await;

        if let Ok(output) = &result {
            record_consumed_read_capacity(&span, output.consumed_capacity.as_ref());
            span.record("aws.dynamodb.scanned_count", output.scanned_count());
            span.record("aws.dynamodb.count", output.count());
            span.record(
                "aws.dynamodb.has_next_page",
                output.last_evaluated_key().is_some(),
            );
        }

        result
    }
}

fn merge_values(l: Option<f64>, r: Option<f64>) -> Option<f64> {
    l.xor(r).or_else(|| l.zip(r).map(|(l, r)| l + r))
}

fn record_consumed_read_capacity(
    span: &tracing::Span,
    consumed_capacity: Option<&ConsumedCapacity>,
) {
    if let Some(consumed_capacity) = consumed_capacity {
        span.record(
            "aws.dynamodb.consumed_read_capacity",
            consumed_capacity
                .read_capacity_units()
                .or(consumed_capacity.capacity_units()),
        );
    }
}

fn record_consumed_write_capacity(
    span: &tracing::Span,
    consumed_capacity: Option<&ConsumedCapacity>,
) {
    if let Some(consumed_capacity) = consumed_capacity {
        span.record(
            "aws.dynamodb.consumed_write_capacity",
            consumed_capacity
                .write_capacity_units()
                .or(consumed_capacity.capacity_units()),
        );
    }
}
