Utilities for working with DynamoDB, particularly focusing on an opinionated
approach to modeling data using single-table design.

In order to effectively use this library, you should first have completed a
single-table design, identifying your entities and the access patterns that you
will need to implement.

# Tables

Tables are defined via a trait implementation. To do this, we'll need to expose
a means for `modyne` to access the table's name, a properly configured client,
and the relevant keys for the table.

Below, we define a database that has one global secondary index in addition to
the default primary key.

```
use modyne::{keys, Table};

struct Database {
    table_name: String,
    client: aws_sdk_dynamodb::Client,
}

impl Table for Database {
    type PrimaryKey = keys::Primary;
    type IndexKeys = keys::Gsi1;

    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn client(&self) -> &aws_sdk_dynamodb::Client {
        &self.client
    }
}
```

## Primary keys and indexes

While a default primary key and generic indexes are provided, it is possible to
define your own primary key or index if desired. These types must be
serde-serializable.

```
use modyne::keys::{
    GlobalSecondaryIndexDefinition, IndexKey, Key, KeyDefinition,
    PrimaryKey, PrimaryKeyDefinition, SecondaryIndexDefinition
};

#[derive(Debug, serde::Serialize)]
struct SessionToken {
    session_token: String,
}

impl PrimaryKey for SessionToken {
    const PRIMARY_KEY_DEFINITION: PrimaryKeyDefinition = PrimaryKeyDefinition {
        hash_key: "session_token",
        range_key: None,
    };
}

impl Key for SessionToken {
    const DEFINITION: KeyDefinition =
        <Self as PrimaryKey>::PRIMARY_KEY_DEFINITION.into_key_definition();
}

#[derive(Debug, serde::Serialize)]
struct UserIndex {
    user_id: String,
}

impl IndexKey for UserIndex {
    const INDEX_DEFINITION: SecondaryIndexDefinition = GlobalSecondaryIndexDefinition {
        index_name: "user_index",
        hash_key: "user_id",
        range_key: None,
    }.into_index();
}
```

## Entities

Entities are the heart of the data model. An instance of an entity represents a
single item in a DynamoDB table. An entity will always have the same primary key
as the assoicated table, but may also participate in zero or more secondary
indexes.

For more information on setting up an entity, see [`derive@EntityDef`] and
[`Entity`].

```
use modyne::{keys, Entity, EntityDef};
#
# struct Database;
#
# impl modyne::Table for Database {
#     type PrimaryKey = keys::Primary;
#     type IndexKeys = keys::Gsi1;
#     fn table_name(&self) -> &str {unimplemented!()}
#     fn client(&self) -> &aws_sdk_dynamodb::Client {unimplemented!()}
# }

#[derive(Debug, EntityDef, serde::Serialize, serde::Deserialize)]
struct Session {
    user_id: String,
    session_token: String,
}

impl Entity for Session {
    type KeyInput<'a> = &'a str;
    type Table = Database;
    type IndexKeys = keys::Gsi1;

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("SESSION#{}", input),
            range: format!("SESSION#{}", input),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(&self.session_token),
            indexes: keys::Gsi1 {
                hash: format!("USER#{}", self.user_id),
                range: format!("SESSION#{}", self.session_token),
            },
        }
    }
}
```

Entities can be interacted with through utility methods on the [`EnitityExt`]
trait.

```
# struct Database;
# impl modyne::Table for Database {
#     type PrimaryKey = modyne::keys::Primary;
#     type IndexKeys = ();
#     fn table_name(&self) -> &str {unimplemented!()}
#     fn client(&self) -> &aws_sdk_dynamodb::Client {unimplemented!()}
# }
# #[derive(Debug, modyne::EntityDef, serde::Serialize)]
# struct Session {user_id: String,session_token: String}
# impl modyne::Entity for Session {
#     type KeyInput<'a> = &'a str;
#     type Table = Database;
#     type IndexKeys = ();
#     fn primary_key(input: Self::KeyInput<'_>) -> modyne::keys::Primary {modyne::keys::Primary { hash: String::new(), range: String::new() }}
#     fn full_key(&self) -> modyne::keys::FullKey<modyne::keys::Primary, Self::IndexKeys> {modyne::keys::FullKey { primary:Self::primary_key(""), indexes:()}}
# }
use modyne::EntityExt;

let mk_session = || Session {
    session_token: String::from("session-1"),
    user_id: String::from("user-1"),
};

let upsert = mk_session().put();
let create = mk_session().create();
let replace = mk_session().replace();
let delete = Session::delete("session-1");
let get = Session::get("session-1");
let update = Session::update("session-1");
```

## Projections

A projection is a read-only view of some subset of an entity's attributes. Every
entity is trivially its own projection. Projections can be defined manually or
by using the [`derive@Projection`] derive macro.

```
use modyne::{EntityDef, Projection};
# struct Database;
# impl modyne::Table for Database {
#     type PrimaryKey = modyne::keys::Primary;
#     type IndexKeys = modyne::keys::Gsi1;
#     fn table_name(&self) -> &str {unimplemented!()}
#     fn client(&self) -> &aws_sdk_dynamodb::Client {unimplemented!()}
# }
#
# impl modyne::Entity for Session {
#     type KeyInput<'a> = &'a str;
#     type Table = Database;
#     type IndexKeys = modyne::keys::Gsi1;
#     fn primary_key(input: Self::KeyInput<'_>) -> modyne::keys::Primary {unimplemented!()}
#     fn full_key(&self) -> modyne::keys::FullKey<modyne::keys::Primary, Self::IndexKeys> {unimplemented!()}
# }

#[derive(Debug, EntityDef, serde::Serialize, serde::Deserialize)]
struct Session {
    user_id: String,
    session_token: String,
}

#[derive(Debug, Projection, serde::Deserialize)]
#[entity(Session)]
struct SessionTokenOnly {
    session_token: String,
}
```

The derive macro for projections includes a minimal amount of verification to
ensure that the field names match names know about from the projected entity.
Note that if the entity or the projection use the `flatten` attribute, then this
detection algorithm will not be able to identify misnamed fields. As an example,
the following will fail to compile.

```compile_fail
# use modyne::{EntityDef, Projection};
# struct Database;
# impl modyne::Table for Database {
#     type PrimaryKey = modyne::keys::Primary;
#     type IndexKeys = modyne::keys::Gsi1;
#     fn table_name(&self) -> &str {unimplemented!()}
#     fn client(&self) -> &aws_sdk_dynamodb::Client {unimplemented!()}
# }
#
# impl modyne::Entity for Session {
#     type KeyInput<'a> = &'a str;
#     type Table = Database;
#     type IndexKeys = modyne::keys::Gsi1;
#     fn primary_key(input: Self::KeyInput<'_>) -> modyne::keys::Primary {unimplemented!()}
#     fn full_key(&self) -> modyne::keys::FullKey<modyne::keys::Primary, Self::IndexKeys> {unimplemented!()}
# }
#
# #[derive(Debug, EntityDef, serde::Serialize, serde::Deserialize)]
# struct Session {
#     user_id: String,
#     session_token: String,
# }
#
#[derive(Debug, Projection, serde::Deserialize)]
#[entity(Session)]
struct SessionTokenOnly {
    session: String,
}
```

However, `serde` field attributes can be used to rename fields so that they will
appropriately match up if a different field name in the struct is desired.

```
# use modyne::{EntityDef, Projection};
# struct Database;
# impl modyne::Table for Database {
#     type PrimaryKey = modyne::keys::Primary;
#     type IndexKeys = modyne::keys::Gsi1;
#     fn table_name(&self) -> &str {unimplemented!()}
#     fn client(&self) -> &aws_sdk_dynamodb::Client {unimplemented!()}
# }
#
# impl modyne::Entity for Session {
#     type KeyInput<'a> = &'a str;
#     type Table = Database;
#     type IndexKeys = modyne::keys::Gsi1;
#     fn primary_key(input: Self::KeyInput<'_>) -> modyne::keys::Primary {unimplemented!()}
#     fn full_key(&self) -> modyne::keys::FullKey<modyne::keys::Primary, Self::IndexKeys> {unimplemented!()}
# }
#
# #[derive(Debug, EntityDef, serde::Serialize, serde::Deserialize)]
# struct Session {
#     user_id: String,
#     session_token: String,
# }
#
#[derive(Debug, Projection, serde::Deserialize)]
#[entity(Session)]
struct SessionTokenOnly {
    #[serde(rename = "session_token")]
    session: String,
}
```

## Aggregates and queries

The most efficient way to pull data out of a DynamoDB table is using range
queries that can extract many entities in one operation. To support this, we
provide means of ergonomically processing the variety of entities that might be
returned in a single query through an aggregate.

```
# struct Database;
# impl modyne::Table for Database {
#     type PrimaryKey = keys::Primary;
#     type IndexKeys = keys::Gsi1;
#     fn table_name(&self) -> &str {unimplemented!()}
#     fn client(&self) -> &aws_sdk_dynamodb::Client {unimplemented!()}
# }
#
# #[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
# struct Session {user_id: String,session_token: String}
# impl modyne::Entity for Session {
#     type KeyInput<'a> = &'a str;
#     type Table = Database;
#     type IndexKeys = modyne::keys::Gsi1;
#     fn primary_key(input: Self::KeyInput<'_>) -> modyne::keys::Primary {unimplemented!()}
#     fn full_key(&self) -> modyne::keys::FullKey<modyne::keys::Primary, Self::IndexKeys> {unimplemented!()}
# }
#
# #[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
# struct UserMetadata {user_id: String}
# impl modyne::Entity for UserMetadata {
#     type KeyInput<'a> = &'a str;
#     type Table = Database;
#     type IndexKeys = modyne::keys::Gsi1;
#     fn primary_key(input: Self::KeyInput<'_>) -> modyne::keys::Primary {unimplemented!()}
#     fn full_key(&self) -> modyne::keys::FullKey<modyne::keys::Primary, Self::IndexKeys> {unimplemented!()}
# }
use modyne::{
    expr, keys, projections, read_projection,
    Aggregate, Error, Item, QueryInput, QueryInputExt,
};

struct UserInfoQuery<'a> {
    user_id: &'a str,
}

impl QueryInput for UserInfoQuery<'_> {
    type Index = keys::Gsi1;
    type Aggregate = UserInfo;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        let partition = format!("USER#{}", self.user_id);
        expr::KeyCondition::in_partition(partition)
    }
}

#[derive(Debug, Default)]
struct UserInfo {
    session_tokens: Vec<String>,
    metadata: Option<UserMetadata>,
}

projections! {
    enum UserInfoEntities {
        Session,
        UserMetadata,
    }
}

impl Aggregate for UserInfo {
    type Projections = UserInfoEntities;

    fn merge(&mut self, item: Item) -> Result<(), Error> {
        match read_projection!(item)? {
            Self::Projections::Session(session) => {
                self.session_tokens.push(session.session_token)
            }
            Self::Projections::UserMetadata(user) => {
                self.metadata = Some(user)
            }
        }
        Ok(())
    }
}

impl Database {
    async fn get_user_info_page(&self, user_id: &str) -> Result<UserInfo, Error> {
        let result = UserInfoQuery { user_id: "test" }
            .query()
            .execute(self)
            .await?;

        let mut info = UserInfo::default();
        info.reduce(result.items.unwrap_or_default())?;
        Ok(info)
    }
}
```
