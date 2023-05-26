//! An example of using modyne to implement a simple single-table
//! database for a session store as described in Chapter 18
//! of the DynamoDB book.

use aliri_braid::braid;
use modyne::{
    expr, keys, types::Expiry, Aggregate, Entity, EntityExt, EntityTypeNameRef, Error, Projection,
    ProjectionExt, QueryInput, QueryInputExt, Table,
};

#[derive(Clone, Debug)]
pub struct App {
    table_name: std::sync::Arc<str>,
    client: aws_sdk_dynamodb::Client,
}

impl App {
    pub fn new(client: aws_sdk_dynamodb::Client) -> Self {
        Self::new_with_table(client, "SessionStore")
    }

    pub fn new_with_table(client: aws_sdk_dynamodb::Client, table_name: &str) -> Self {
        Self {
            table_name: std::sync::Arc::from(table_name),
            client,
        }
    }
}

impl Table for App {
    type PrimaryKey = SessionToken;
    type IndexKeys = UsernameKey;

    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn client(&self) -> &aws_sdk_dynamodb::Client {
        &self.client
    }
}

impl App {
    pub async fn create_session(&self, session: Session) -> Result<(), Error> {
        session.create().execute(self).await?;
        Ok(())
    }

    pub async fn get_session(&self, session_token: uuid::Uuid) -> Result<Option<Session>, Error> {
        let now = time::OffsetDateTime::now_utc();
        self.get_session_with_now(session_token, now).await
    }

    pub async fn get_session_with_now(
        &self,
        session_token: uuid::Uuid,
        now: time::OffsetDateTime,
    ) -> Result<Option<Session>, Error> {
        let result = Session::get(session_token).execute(self).await?;
        if let Some(item) = result.item {
            let session = Session::from_item(item)?;
            if session.expires_at > now {
                Ok(Some(session))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn delete_user_sessions(&self, user: &UsernameRef) -> Result<(), Error> {
        let mut joiner = tokio::task::JoinSet::new();
        loop {
            let mut agg = Vec::<SessionTokenOnly>::new();

            let result = user.query().execute(self).await?;

            agg.reduce(result.items.unwrap_or_default())?;

            for session in agg {
                let this = self.clone();
                joiner.spawn(
                    async move { Session::delete(session.session_token).execute(&this).await },
                );
            }

            if result.last_evaluated_key.is_none() {
                break;
            }
        }

        let mut last_result = Ok(());

        while let Some(next) = joiner.join_next().await {
            match next {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => {
                    tracing::error!(
                        exception = &err as &dyn std::error::Error,
                        "error while deleting session"
                    );
                    last_result = Err(err);
                }
                Err(err) => {
                    tracing::error!(
                        exception = &err as &dyn std::error::Error,
                        "panic while deleting session"
                    );
                }
            }
        }

        Ok(last_result?)
    }
}

#[braid(serde)]
pub struct Username;

#[derive(Clone, Debug, serde::Serialize)]
pub struct SessionToken {
    pub session_token: uuid::Uuid,
}

impl keys::PrimaryKey for SessionToken {
    const PRIMARY_KEY_DEFINITION: keys::PrimaryKeyDefinition = keys::PrimaryKeyDefinition {
        hash_key: "session_token",
        range_key: None,
    };
}

impl keys::Key for SessionToken {
    const DEFINITION: keys::KeyDefinition =
        <Self as keys::PrimaryKey>::PRIMARY_KEY_DEFINITION.into_key_definition();
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct UsernameKey {
    pub username: Username,
}

impl keys::IndexKey for UsernameKey {
    const INDEX_DEFINITION: keys::SecondaryIndexDefinition = keys::GlobalSecondaryIndexDefinition {
        index_name: "UserIndex",
        hash_key: "username",
        range_key: None,
    }
    .into_index();
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Session {
    pub session_token: uuid::Uuid,
    pub username: Username,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: time::OffsetDateTime,
    pub ttl: Expiry,
}

impl Entity for Session {
    const ENTITY_TYPE: &'static EntityTypeNameRef = EntityTypeNameRef::from_static("session");
    const PROJECTED_ATTRIBUTES: &'static [&'static str] = &[
        "session_token",
        "username",
        "created_at",
        "expires_at",
        "ttl",
    ];

    type KeyInput<'a> = uuid::Uuid;
    type Table = App;
    type IndexKeys = UsernameKey;

    fn primary_key(input: Self::KeyInput<'_>) -> SessionToken {
        SessionToken {
            session_token: input,
        }
    }

    fn full_key(&self) -> keys::FullKey<SessionToken, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(self.session_token),
            indexes: UsernameKey {
                username: self.username.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct SessionTokenOnly {
    pub session_token: uuid::Uuid,
}

impl Projection for SessionTokenOnly {
    type Entity = Session;
    const PROJECTED_ATTRIBUTES: &'static [&'static str] = &["session_token"];
}

impl QueryInput for UsernameRef {
    type Index = UsernameKey;
    type Aggregate = Vec<SessionTokenOnly>;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        expr::KeyCondition::in_partition(self)
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::TimeToLiveSpecification;
    use modyne::{
        model::{BatchGet, BatchWrite},
        TestTableExt,
    };

    use super::*;

    #[test_log::test(tokio::test)]
    #[ignore = "this test requires a local DynamoDB instance running on localhost:4566 and may be \
                slow"]
    async fn localstack_only_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = aws_config::from_env()
            .endpoint_url("http://localhost:4566")
            .credentials_provider(aws_credential_types::Credentials::new(
                "test", "test", None, None, "static",
            ))
            .load()
            .await;
        let client = aws_sdk_dynamodb::Client::new(&config);
        let app = App::new(client);

        let _ = app.delete_table().send().await;

        let _create_table = app.create_table().send().await?;

        app.client()
            .update_time_to_live()
            .table_name(app.table_name())
            .time_to_live_specification(
                TimeToLiveSpecification::builder()
                    .attribute_name("ttl")
                    .enabled(true)
                    .build(),
            )
            .send()
            .await?;

        let mut handles = tokio::task::JoinSet::new();
        for i in 0..100 {
            let app = app.clone();
            handles.spawn(async move {
                app.create_session(Session {
                    session_token: uuid::Uuid::new_v4(),
                    username: Username::from(format!("mtest_{}", i % 13)),
                    created_at: time::OffsetDateTime::now_utc(),
                    expires_at: time::OffsetDateTime::now_utc(),
                    ttl: Expiry::from(time::OffsetDateTime::now_utc()),
                })
                .await
            });
        }
        while handles
            .join_next()
            .await
            .transpose()?
            .transpose()?
            .is_some()
        {}

        app.delete_user_sessions(UsernameRef::from_static("mtest_6"))
            .await?;

        let session_token = uuid::Uuid::new_v4();

        let now = time::OffsetDateTime::now_utc();
        let expires = time::OffsetDateTime::now_utc() + time::Duration::hours(5);
        app.create_session(Session {
            session_token,
            username: Username::from("session_test"),
            created_at: now,
            expires_at: expires,
            ttl: expires.into(),
        })
        .await?;

        let session = app.get_session_with_now(session_token, now).await?.unwrap();
        assert_eq!(session.username, UsernameRef::from_static("session_test"));

        let session = app.get_session_with_now(session_token, expires).await?;
        assert!(session.is_none());

        Ok(())
    }

    #[test_log::test(tokio::test)]
    #[ignore = "this test requires a local DynamoDB instance running on localhost:4566 and may be \
                slow"]
    async fn batch_put_get_delete() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = aws_config::from_env()
            .endpoint_url("http://localhost:4566")
            .credentials_provider(aws_credential_types::Credentials::new(
                "test", "test", None, None, "static",
            ))
            .load()
            .await;
        let client = aws_sdk_dynamodb::Client::new(&config);
        let app = App::new_with_table(client, "SessionStore_BatchTest");

        let _ = app.delete_table().send().await;

        let _create_table = app.create_table().send().await?;

        app.client()
            .update_time_to_live()
            .table_name(app.table_name())
            .time_to_live_specification(
                TimeToLiveSpecification::builder()
                    .attribute_name("ttl")
                    .enabled(true)
                    .build(),
            )
            .send()
            .await?;

        const WRITE_BATCH_SIZE: usize = 25;
        const READ_BATCH_SIZE: usize = 100;
        let mut i = 0;
        let operations = std::iter::from_fn(move || {
            i += 1;
            let put = Session {
                session_token: uuid::Uuid::new_v4(),
                username: Username::from(format!("mtest_{}", i % 7)),
                created_at: time::OffsetDateTime::now_utc(),
                expires_at: time::OffsetDateTime::now_utc(),
                ttl: Expiry::from(time::OffsetDateTime::now_utc()),
            };
            Some(put)
        })
        .take(READ_BATCH_SIZE + 29)
        .collect::<Vec<_>>();

        for b in operations.chunks(WRITE_BATCH_SIZE) {
            let mut batch = BatchWrite::new();
            for op in b {
                batch = batch.operation(op.clone().put());
            }
            let result = batch.execute(&app).await?;

            assert!(result.unprocessed_items.unwrap_or_default().is_empty());
        }

        for b in operations.chunks(READ_BATCH_SIZE) {
            let mut batch = BatchGet::new();
            for op in b {
                batch = batch.operation(Session::get(op.session_token));
            }
            let result = batch.execute(&app).await?;

            assert_eq!(result.responses().map(|r| r.len()).unwrap_or_default(), 1);
            assert_eq!(
                result
                    .responses()
                    .and_then(|r| r.get(app.table_name()))
                    .map(|t| t.len())
                    .unwrap_or_default(),
                b.len()
            );
            assert!(result.unprocessed_keys.unwrap_or_default().is_empty());
        }

        for b in operations.chunks(WRITE_BATCH_SIZE) {
            let mut batch = BatchWrite::new();
            for op in b {
                batch = batch.operation(Session::delete(op.session_token));
            }
            let result = batch.execute(&app).await?;

            assert!(result.unprocessed_items.unwrap_or_default().is_empty());
        }

        Ok(())
    }
}
