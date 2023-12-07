#![doc = include_str!("../README.md")]

use aliri_braid::braid;
use modyne::{
    expr, keys, types::Expiry, Aggregate, Entity, EntityDef, EntityExt, Error, Projection,
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
    /// For demonstration, this example uses a non-standard entity type attribute name
    const ENTITY_TYPE_ATTRIBUTE: &'static str = "et";

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

#[derive(Clone, Debug, EntityDef, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, Projection, serde::Deserialize)]
#[entity(Session)]
pub struct SessionTokenOnly {
    pub session_token: uuid::Uuid,
}

impl QueryInput for UsernameRef {
    type Index = UsernameKey;
    type Aggregate = Vec<SessionTokenOnly>;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        expr::KeyCondition::in_partition(self)
    }
}
