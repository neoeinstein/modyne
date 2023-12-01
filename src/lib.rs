#![doc = include_str!("../docs/modyne.md")]
#![warn(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rustdoc::broken_intra_doc_links)]

mod error;
pub mod expr;
pub mod keys;
pub mod model;
pub mod types;

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use keys::{IndexKeys, PrimaryKey};
use model::{ConditionCheck, ConditionalPut, Delete, Get, Put, Query, Scan, Update};
/// Derive macro for the [`trait@EntityDef`] trait
///
/// This macro piggy-backs on the attributes used by the `serde_derive`
/// crate. Note that using `flatten` will result in an empty projection
/// expression, pulling _all_ attributes on the item because this macro
/// cannot identify the field names used in the flattened structure.
#[cfg(feature = "derive")]
pub use modyne_derive::EntityDef;
/// Derive macro for the [`trait@Projection`] trait
///
/// Like [`derive@EntityDef`], this macro piggy-backs on the attributes used by
/// the `serde_derive` crate. Note that using `flatten` will result in
/// an empty projection expression, pulling _all_ attributes on the item
/// because this macro cannot identify the field names used in the
/// flattened structure.
///
/// Usage of this macro requires specifying the "parent" entity. For
/// example, with an entity called `MyEntity`, the projection should
/// have the following attribute: `#[entity(MyEntity)]`
#[cfg(feature = "derive")]
pub use modyne_derive::Projection;
use serde_dynamo::aws_sdk_dynamodb_1 as codec;

pub use crate::error::Error;

const ENTITY_TYPE_ATTRIBUTE: &str = "entity_type";

/// An alias for a DynamoDB item
pub type Item = HashMap<String, AttributeValue>;

/// The name for a DynamoDB entity type
#[aliri_braid::braid(serde)]
pub struct EntityTypeName;

/// A description of a DynamoDB table
pub trait Table {
    /// The primary key to be used for the table
    type PrimaryKey: keys::PrimaryKey;

    /// Indexes defined on the table
    type IndexKeys: IndexKeys;

    /// Returns the name of the DynamoDB table
    fn table_name(&self) -> &str;

    /// Returns a reference to the DynamoDB client used by this table
    fn client(&self) -> &aws_sdk_dynamodb::Client;
}

/// The name and attribute definition for an [`Entity`]
///
/// This trait is used to define the structure of an entity type in a
/// DynamoDB table and how the entity may be queried.
///
/// This trait can be implemented manually, but may be better implemented
/// using the [`derive@EntityDef`] derive macro exposed when
/// using the `derive` feature on this crate. Manual implementation may
/// lead to the projected attributes going out of sync with the
/// entity's attributes.
///
/// ## Example
///
/// ```
/// use modyne::EntityDef;
///
/// #[derive(EntityDef)]
/// #[serde(rename = "orange", rename_all = "kebab-case")]
/// struct MyStruct {
///     field_1: u32,
///     #[serde(rename = "second-field")]
///     field_2: u32,
/// }
/// ```
///
/// The above is equivalent to the following manual definition:
///
/// ```
/// use modyne::{EntityDef, EntityTypeNameRef};
///
/// struct MyStruct {
///     field_1: u32,
///     field_2: u32,
/// }
///
/// impl EntityDef for MyStruct {
///     const ENTITY_TYPE: &'static EntityTypeNameRef =
///         EntityTypeNameRef::from_static("orange");
///
///     const PROJECTED_ATTRIBUTES: &'static [&'static str] = &[
///         "field_1",
///         "second-field",
///     ];
/// }
/// ```
///
/// If a field is marked with serde's `flatten` modifier, then the projected
/// attributes array will be empty due to the inability of the derive macro
/// to inspect the fields that are available on the flattened type.
pub trait EntityDef {
    /// The name of the entity type
    ///
    /// This value will be used to set the `entity_type` attribute on
    /// all items of this entity type in the DynamoDB table and should
    /// be unique across all entity types in the table.
    const ENTITY_TYPE: &'static EntityTypeNameRef;

    /// The set of attributes that are projected into the entity
    ///
    /// By default, all attributes, including the index keys, are
    /// projected into the entity. This can be overridden to only
    /// project the subset of attributes that are needed for the
    /// entity's use cases.
    ///
    /// Use of this attribute is optional, but recommended. If not
    /// specified, then any aggregate that uses this entity type will
    /// return the entire item from DynamoDB, which can lead to
    /// unnecessary network and deserialization overhead.
    const PROJECTED_ATTRIBUTES: &'static [&'static str] = &[];
}

/// An entity in a DynamoDB table
///
/// This trait is used to define the structure of an entity type in a
/// DynamoDB table and how the entity may be queried.
///
/// Projections of the entity can be defined using the [`Projection`] trait.
///
/// # Example
///
/// Here we define a simple order entity type. To support write patterns, the
/// order's primary key only requires the order's ID. However, to support an
/// access pattern where we want to query all orders for a given user, we
/// define a global secondary index with a partition key of `USER#<user_id>`
/// and a sort key that includes the order's date, which allows us to more
/// efficiently query for recent orders for a given user.
///
/// ```
/// use modyne::{keys, Entity, EntityDef};
/// # use time::format_description::well_known::Rfc3339;
/// #
/// # struct App;
/// # impl modyne::Table for App {
/// #     type PrimaryKey = keys::Primary;
/// #     type IndexKeys = keys::Gsi1;
/// #     fn table_name(&self) -> &str { unimplemented!() }
/// #     fn client(&self) -> &aws_sdk_dynamodb::Client { unimplemented!() }
/// # }
///
/// #[derive(Debug, EntityDef, serde::Serialize, serde::Deserialize)]
/// struct Order {
///     user_id: String,
///     order_id: String,
///     #[serde(with = "time::serde::rfc3339")]
///     order_date: time::OffsetDateTime,
///     items: Vec<OrderItem>,
/// }
///
/// #[derive(Debug, serde::Serialize, serde::Deserialize)]
/// struct OrderItem {
///     item_id: String,
///     quantity: u32,
/// }
///
/// struct OrderKeyInput<'a> {
///     order_id: &'a str,
/// }
///
/// impl Entity for Order {
///     type KeyInput<'a> = OrderKeyInput<'a>;
///     type Table = App;
///     type IndexKeys = keys::Gsi1;
///
///     fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
///         keys::Primary {
///             hash: format!("ORDER#{}", input.order_id),
///             range: format!("ORDER#{}", input.order_id),
///         }
///     }
///
///     fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
///         let order_date = self.order_date.format(&Rfc3339).unwrap();
///         keys::FullKey {
///             primary: Self::primary_key(OrderKeyInput { order_id: &self.order_id }),
///             indexes: keys::Gsi1 {
///                  hash: format!("USER#{}", self.user_id),
///                  range: format!("ORDER#{}", order_date),
///             },
///         }
///     }
/// }
/// ```
pub trait Entity: EntityDef + Sized {
    /// The inputs required to generate the entity's primary key.
    ///
    /// This can be a single type or a tuple of types. Note that all
    /// the values required to generate the primary key must also be
    /// attributes of the entity itself.
    ///
    /// These input should be chosen to support write patterns for the
    /// entity, preferrably so that the primary key can be generated
    /// without having to read the entity from the database using a
    /// secondary index.
    ///
    /// # Example
    ///
    /// Using a structured type as the input for the primary key:
    ///
    /// ```
    /// struct MyInputs<'a> {
    ///     item_id: &'a str,
    ///     order_date: time::OffsetDateTime,
    /// }
    ///
    /// type KeyInput<'a> = MyInputs<'a>;
    /// ```
    ///
    /// Using a tuple as the input for the primary key:
    ///
    /// ```
    /// type KeyInput<'a> = (&'a str, time::OffsetDateTime);
    /// ```
    type KeyInput<'a>;

    /// The primary key for the entity
    ///
    /// Often this should be [`keys::Primary`] unless using a different
    /// primary key type.
    type Table: Table;

    /// The set of keys used to index the entity
    ///
    /// Multiple keys can be specified to support multiple indexes. Many types
    /// of index keys have already been defined in the [`keys`] module.
    ///
    /// # Example
    ///
    /// Using a single index key:
    ///
    /// ```
    /// # use modyne::keys;
    /// type IndexKeys = keys::Gsi1;
    /// ```
    ///
    /// Specifying a two global secondary indexes:
    ///
    /// ```
    /// # use modyne::keys;
    /// type IndexKeys = (keys::Gsi1, keys::Gsi2);
    /// ```
    type IndexKeys: keys::IndexKeys;

    /// Generate the primary key for an entity
    ///
    /// This is primarily used when retrieving, updating, or deleting a
    /// specific entity.
    fn primary_key(input: Self::KeyInput<'_>) -> <Self::Table as Table>::PrimaryKey;

    /// Generate the full set of keys for an entity
    ///
    /// This is primarily used when upserting an entity into the database.
    fn full_key(&self) -> keys::FullKey<<Self::Table as Table>::PrimaryKey, Self::IndexKeys>;
}

/// Extension trait for [`Entity`] types
pub trait EntityExt: Entity {
    /// The definition for the entity's primary key
    const KEY_DEFINITION: keys::PrimaryKeyDefinition =
        <<Self::Table as Table>::PrimaryKey as keys::PrimaryKey>::PRIMARY_KEY_DEFINITION;

    /// Convert the entity into a DynamoDB item
    ///
    /// The generated item will include all of the entity's attributes, as well
    /// as the entity type and all index key attributes.
    fn into_item(self) -> Item
    where
        Self: serde::Serialize,
    {
        let full_entity = FullEntity {
            entity_type: Self::ENTITY_TYPE,
            keys: self.full_key(),
            entity: self,
        };

        crate::codec::to_item(full_entity).unwrap()
    }

    /// Prepares a get operation for the entity
    #[inline]
    fn get(input: Self::KeyInput<'_>) -> Get {
        Get::new(Self::primary_key(input).into_key())
    }

    /// Prepares a put operation for the entity
    #[inline]
    fn put(self) -> Put
    where
        Self: serde::Serialize,
    {
        Put::new(self.into_item())
    }

    /// Prepares a put operation for the entity that requires that
    /// no entity already exist with the same key
    #[inline]
    fn create(self) -> ConditionalPut
    where
        Self: serde::Serialize,
    {
        let condition = expr::Condition::new("attribute_not_exists(#PK)").name(
            "#PK",
            <<Self::Table as Table>::PrimaryKey as keys::PrimaryKey>::PRIMARY_KEY_DEFINITION
                .hash_key,
        );
        self.put().condition(condition)
    }

    /// Prepares a put operation for the entity that requires that
    /// an entity already exist with the same key
    #[inline]
    fn replace(self) -> ConditionalPut
    where
        Self: serde::Serialize,
    {
        let condition = expr::Condition::new("attribute_exists(#PK)").name(
            "#PK",
            <<Self::Table as Table>::PrimaryKey as keys::PrimaryKey>::PRIMARY_KEY_DEFINITION
                .hash_key,
        );
        self.put().condition(condition)
    }

    /// Prepares an update operation for the entity
    ///
    /// # Note
    ///
    /// If this update would change an attribute that is used in the creation of a key attribute,
    /// that key attribute must also be explicitly updated. In cases where the entire state of the
    /// entity is known, using a [`replace()`][EntityExt::replace()] may be better, as that will
    /// also update any computed key attributes.
    #[inline]
    fn update(key: Self::KeyInput<'_>) -> Update {
        Update::new(Self::primary_key(key).into_key())
    }

    /// Prepares a delete operation for the entity
    #[inline]
    fn delete(key: Self::KeyInput<'_>) -> Delete {
        Delete::new(Self::primary_key(key).into_key())
    }

    /// Prepares a condition check operation for the entity, for transactional writes
    #[inline]
    fn condition_check(key: Self::KeyInput<'_>, condition: expr::Condition) -> ConditionCheck {
        ConditionCheck::new(Self::primary_key(key).into_key(), condition)
    }
}

impl<T: Entity> EntityExt for T {}

/// A projection of an entity that may not contain all of the entity's attributes
///
/// This trait can be used when querying a subset of an entity's attributes. In this way
/// time won't be spent deserializing attributes that aren't needed.
///
/// Note that this type does not automatically impose a projection expression on the DynamoDB
/// operation, so network bandwidth will still be spent retrieving the full entity unless the
/// projected attributes are specified.
///
/// In addition, even if a projection expression is specified, the full size of an item will
/// still be counted when computing the DynamoDB read capacity unit consumption.
///
/// For easier implementation, use the [`derive@Projection`] derive macro to infer the projected
/// attributes automatically.
pub trait Projection: Sized {
    /// The set of attributes that are projected into the entity.
    ///
    /// By default, the set of projected attributes defined on the entity
    /// will be projected.
    ///
    /// Use of this attribute is optional, but recommended. If not
    /// specified here or on the entity, then any aggregate that uses
    /// this projection will return the entire item from DynamoDB, which
    /// can lead to unnecessary network and deserialization overhead.
    const PROJECTED_ATTRIBUTES: &'static [&'static str] =
        <Self::Entity as EntityDef>::PROJECTED_ATTRIBUTES;

    /// The entity type that this projection represents
    type Entity: Entity;
}

impl<T> Projection for T
where
    T: Entity,
{
    type Entity = Self;
}

/// Extension trait for [`Projection`] types
pub trait ProjectionExt: Projection {
    /// Deserialize a DynamoDB item into this projection
    fn from_item(item: Item) -> Result<Self, Error>;
}

impl<'a, P> ProjectionExt for P
where
    P: Projection + serde::Deserialize<'a>,
{
    fn from_item(item: Item) -> Result<Self, Error> {
        let parsed = crate::codec::from_item(item).map_err(|error| {
            crate::error::ItemDeserializationError::new(Self::Entity::ENTITY_TYPE, error)
        })?;

        Ok(parsed)
    }
}

/// A description of the set of entity types that constitute an [`Aggregate`]
///
/// This trait is not generally implemented directly, but rather is generated
/// by using the [`projections!`] macro.
pub trait ProjectionSet: Sized {
    /// Attempt to parse an known entity from a DynamoDB item
    ///
    /// On an unknown entity type, this method should return `Ok(None)`.
    ///
    /// # Errors
    ///
    /// This method will return an error if the item cannot be parsed
    /// based on the entity type that is present in the item or if the
    /// entity type attribute is missing from the item.
    fn try_from_item(item: Item) -> Result<Option<Self>, Error>;

    /// Generate a projection expression for the aggregate
    ///
    /// This expression will include all of the attributes that are
    /// projected by any of the entity types in the aggregate.
    fn projection_expression() -> Option<expr::StaticProjection>;
}

/// Utility macro for defining an [`ProjectionSet`] used when querying items
/// into an [`Aggregate`]
///
/// See the [module-level documentation][crate] for more details.
#[macro_export]
macro_rules! projections {
    ($(#[$meta:meta])* $v:vis enum $name:ident { $($ty:ident),* $(,)? }) => {
        $(#[$meta])*
        $v enum $name {
            $($ty($ty),)*
        }

        impl $crate::ProjectionSet for $name {
            fn try_from_item(item: $crate::Item) -> ::std::result::Result<::std::option::Option<Self>, $crate::Error> {
                let entity_type = $crate::__private::get_entity_type(&item)?;

                let parsed =
                $(
                    if entity_type == <<$ty as $crate::Projection>::Entity as $crate::EntityDef>::ENTITY_TYPE {
                        let parsed = <$ty as $crate::ProjectionExt>::from_item(item)
                            .map(Self::$ty)?;
                        ::std::option::Option::Some(parsed)
                    } else
                )*
                {
                    tracing::warn!(entity_type = entity_type.as_str(), "unknown entity type");
                    ::std::option::Option::None
                };

                ::std::result::Result::Ok(parsed)
            }

            fn projection_expression() -> ::std::option::Option<$crate::expr::StaticProjection> {
                $crate::once_projection_expression!($($ty),*)
            }
        }
    };
}

/// Generate a static projection expression that is computed exactly once in the lifetime
/// of the program
///
/// This may be used when overriding the implementations for the projection expression
/// in [`ScanInput`][ScanInput::projection_expression()] if desired.
///
/// # Example
///
/// ```
/// # struct Database;
/// # impl modyne::Table for Database {
/// #     type PrimaryKey = modyne::keys::Primary;
/// #     type IndexKeys = modyne::keys::Gsi1;
/// #     fn table_name(&self) -> &str {unimplemented!()}
/// #     fn client(&self) -> &aws_sdk_dynamodb::Client {unimplemented!()}
/// # }
/// #
/// # struct User {}
/// # impl modyne::EntityDef for User {
/// #     const ENTITY_TYPE: &'static modyne::EntityTypeNameRef = modyne::EntityTypeNameRef::from_static("user");
/// #     const PROJECTED_ATTRIBUTES: &'static [&'static str] = &["user_id"];
/// # }
/// # impl modyne::Entity for User {
/// #     type KeyInput<'a> = &'a str;
/// #     type Table = Database;
/// #     type IndexKeys = modyne::keys::Gsi1;
/// #     fn primary_key(input: Self::KeyInput<'_>) -> modyne::keys::Primary {unimplemented!()}
/// #     fn full_key(&self) -> modyne::keys::FullKey<modyne::keys::Primary, Self::IndexKeys> {unimplemented!()}
/// # }
/// use modyne::{expr, keys, once_projection_expression, ScanInput};
/// struct UserIndexScan;
///
/// impl ScanInput for UserIndexScan {
///     type Index = keys::Gsi1;
///
///     fn projection_expression() -> Option<expr::StaticProjection> {
///         once_projection_expression!(User)
///     }
/// }
/// ```
#[macro_export]
macro_rules! once_projection_expression {
    ($($ty:path),* $(,)?) => {{
        const PROJECTIONS: &'static [&'static [&'static str]] = &[
            $(
                <$ty as $crate::Projection>::PROJECTED_ATTRIBUTES,
            )*
        ];

        static PROJECTION_ONCE: $crate::__private::OnceLock<
            ::std::option::Option<$crate::expr::StaticProjection>,
        > = $crate::__private::OnceLock::new();

        *PROJECTION_ONCE.get_or_init(|| {
            $crate::__private::generate_projection_expression(PROJECTIONS)
        })
    }};
}

/// Utility macro for reading an entity from a DynamoDB item
///
/// The projection set is inferred from the context in which this macro is used.
/// If an projection type is not present in the projection set, then the macro will
/// short-circuit to skip the item.
///
/// This macro is generally used in the implementation of [`Aggregate::merge`],
/// to ergonomically parse an entity from a DynamoDB item. Use outside of this
/// context is unlikely to compile.
#[macro_export]
macro_rules! read_projection {
    ($item:expr) => {{
        match <Self::Projections as $crate::ProjectionSet>::try_from_item($item) {
            Ok(Some(entity)) => Ok(entity),
            Ok(None) => return Ok(()),
            Err(error) => Err(error),
        }
    }};
}

/// An aggregate of multiple entity types, often used when querying multiple
/// items from a single partition key.
pub trait Aggregate: Default {
    /// The set of entity types that are expected to be returned from the aggregate
    ///
    /// This type is usually generated using the [`projections!`] macro.
    type Projections: ProjectionSet;

    /// Extends the aggregate with the entities represented by the given items
    fn reduce<I>(&mut self, items: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = Item>,
    {
        for item in items {
            self.merge(item)?;
        }

        Ok(())
    }

    /// Merges the entity represented by the given item into the aggregate
    ///
    /// When implementing this method, it is recommended to use the [`read_projection!`]
    /// macro, which will deserialize the item into the correct entity type,
    /// ignoring any unknown entity types.
    fn merge(&mut self, item: Item) -> Result<(), Error>;
}

impl<'a, P> ProjectionSet for P
where
    P: Projection + serde::Deserialize<'a> + 'static,
{
    fn try_from_item(item: Item) -> Result<Option<Self>, Error> {
        match item.get(ENTITY_TYPE_ATTRIBUTE) {
            Some(AttributeValue::S(entity_type)) => {
                let entity_type = EntityTypeNameRef::from_str(entity_type);
                if entity_type == <P::Entity as EntityDef>::ENTITY_TYPE {
                    let parsed = P::from_item(item)?;
                    Ok(Some(parsed))
                } else {
                    tracing::warn!(entity_type = entity_type.as_str(), "unknown entity type");
                    Ok(None)
                }
            }
            _ => Err(crate::error::MissingEntityTypeError {}.into()),
        }
    }

    fn projection_expression() -> Option<expr::StaticProjection> {
        use std::{any::TypeId, collections::BTreeMap, sync::RwLock};

        static ENTITY_PROJECTION_EXPRESSION: RwLock<
            BTreeMap<TypeId, Option<expr::StaticProjection>>,
        > = RwLock::new(BTreeMap::new());

        // Optimistically take a read lock to see if we've already computed the projection
        {
            let projections = ENTITY_PROJECTION_EXPRESSION.read().unwrap();
            if let Some(&projection) = projections.get(&TypeId::of::<P>()) {
                return projection;
            }
        }

        // If we didn't find the projection, take a write lock and compute it
        let mut projections = ENTITY_PROJECTION_EXPRESSION.write().unwrap();
        *projections.entry(TypeId::of::<P>()).or_insert_with(|| {
            // If the entity type doesn't have any projected attributes, then we can't
            // generate a projection expression. This then means that _all_ attributes
            // will be returned.
            if !P::PROJECTED_ATTRIBUTES.iter().all(|a| !a.is_empty()) {
                return None;
            }

            let projection = expr::Projection::new(
                P::PROJECTED_ATTRIBUTES
                    .iter()
                    .copied()
                    .chain([ENTITY_TYPE_ATTRIBUTE]),
            );

            // Leak the generated projection expression. This is safe since we're the
            // only ones with a lock that allows generating an expression. Thus no unnecessary
            // expressions will be generated (only one expression per projection; no
            // unbounded leaks). This expression will then be reused for the rest of the
            // process lifetime.
            Some(projection.leak())
        })
    }
}

impl<'a, P> Aggregate for Vec<P>
where
    P: Projection + serde::Deserialize<'a> + 'static,
{
    type Projections = P;

    fn reduce<I>(&mut self, items: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = Item>,
    {
        let items = items.into_iter();
        self.reserve(items.size_hint().0);
        for item in items {
            self.merge(item)?;
        }

        Ok(())
    }

    fn merge(&mut self, item: Item) -> Result<(), Error> {
        let entity = read_projection!(item)?;
        self.push(entity);
        Ok(())
    }
}

/// A value that can be used to query an aggregate
pub trait QueryInput {
    /// Whether to use consistent reads for the query
    const CONSISTENT_READ: bool = false;

    /// Whether to scan the index forward
    const SCAN_INDEX_FORWARD: bool = true;

    /// The index used to query the aggregate
    type Index: keys::Key;

    /// The aggregate that this query is for
    type Aggregate: Aggregate;

    /// The key condition to apply on this query
    fn key_condition(&self) -> expr::KeyCondition<Self::Index>;

    /// Specify which items should be returned by the query
    ///
    /// This is a filter expression that is applied to items after reading but
    /// before returning. Items scanned but not returned by the filter
    /// expression will still be counted towards any limit and read
    /// capacity quotas.
    ///
    /// Where possible, it is preferrable to rely on the key condition to
    /// filter the set of items returned, as that will be more efficient.
    #[inline]
    fn filter_expression(&self) -> Option<expr::Filter> {
        None
    }
}

/// Extensions to an aggregate query
pub trait QueryInputExt: QueryInput {
    /// Prepare a DynamoDB query
    ///
    /// This will prepare a query operation for the input, applying
    /// the key condition, filter expression, read consistency,
    /// and scan direction as defined by the input. Additional settings can
    /// be applied by chaining methods on the returned [`Query`] value.
    fn query(&self) -> Query<Self::Index>;
}

impl<Q> QueryInputExt for Q
where
    Q: QueryInput + ?Sized,
{
    fn query(&self) -> Query<Self::Index> {
        let mut query = Query::new(self.key_condition());

        if let Some(projection) =
            <<Self as QueryInput>::Aggregate as Aggregate>::Projections::projection_expression()
        {
            query = query.projection(projection);
        }

        if let Some(filter) = self.filter_expression() {
            query = query.filter(filter);
        }

        if Self::CONSISTENT_READ {
            query = query.consistent_read();
        }

        if !Self::SCAN_INDEX_FORWARD {
            query = query.scan_index_backward();
        }

        query
    }
}

/// A value that can be used to query an aggregate
pub trait ScanInput {
    /// Whether to use consistent reads for the scan
    const CONSISTENT_READ: bool = false;

    /// The index to be scanned
    type Index: keys::Key;

    /// Specify which items should be returned by the scan
    ///
    /// This is a filter expression that is applied to items after reading but
    /// before returning. Items scanned but not returned by the filter
    /// expression will still be counted towards any limit and read
    /// capacity quotas.
    #[inline]
    fn filter_expression(&self) -> Option<expr::Filter> {
        None
    }

    /// Specify which attributes should be returned by the scan
    ///
    /// This is a projection expression that is applied to items being
    /// returned. The full size of an item is counted toward read
    /// capacity usage, regardless of which attributes are returned.
    ///
    /// The [`once_projection_expression!`] macro can be used to automatically
    /// generate a projection expression from a known set of entities that
    /// the scan will return.
    #[inline]
    fn projection_expression() -> Option<expr::StaticProjection> {
        None
    }
}

/// Extensions to an aggregate scan
pub trait ScanInputExt: ScanInput {
    /// Prepare a DynamoDB scan
    ///
    /// This will prepare a scan operation for the input, applying
    /// filter expression and consistent read settings as defined by the input.
    /// Additional settings can be applied by chaining methods
    /// on the returned [`Scan`] value.
    fn scan(&self) -> Scan<Self::Index>;
}

impl<S> ScanInputExt for S
where
    S: ScanInput + ?Sized,
{
    fn scan(&self) -> Scan<Self::Index> {
        let mut scan = Scan::new();

        if let Some(filter) = self.filter_expression() {
            scan = scan.filter(filter);
        }

        if let Some(projection) = Self::projection_expression() {
            scan = scan.projection(projection)
        }

        if Self::CONSISTENT_READ {
            scan = scan.consistent_read();
        }

        scan
    }
}

#[derive(serde::Serialize)]
struct FullEntity<T: Entity> {
    entity_type: &'static EntityTypeNameRef,

    #[serde(flatten)]
    keys: keys::FullKey<<T::Table as Table>::PrimaryKey, T::IndexKeys>,

    #[serde(flatten)]
    entity: T,
}

#[doc(hidden)]
pub mod __private {
    #[cfg(not(feature = "once_cell"))]
    pub type OnceLock<T> = std::sync::OnceLock<T>;

    #[cfg(feature = "once_cell")]
    pub type OnceLock<T> = once_cell::sync::OnceCell<T>;

    #[inline]
    pub fn get_entity_type(item: &crate::Item) -> Result<&crate::EntityTypeNameRef, crate::Error> {
        let entity_type = item
            .get(crate::ENTITY_TYPE_ATTRIBUTE)
            .ok_or(crate::error::MissingEntityTypeError {})?
            .as_s()
            .map_err(|_| crate::error::MissingEntityTypeError {})?
            .as_str();
        Ok(crate::EntityTypeNameRef::from_str(entity_type))
    }

    /// Generate a projection expression for the given entity types
    pub fn generate_projection_expression(
        attributes: &[&[&str]],
    ) -> Option<crate::expr::StaticProjection> {
        if !attributes.iter().all(|attrs| !attrs.is_empty()) {
            return None;
        }

        let expr = crate::expr::Projection::new(
            attributes
                .iter()
                .copied()
                .flatten()
                .copied()
                .chain([crate::ENTITY_TYPE_ATTRIBUTE]),
        );
        Some(expr.leak())
    }
}

/// Extension trait for [`Table`] to provide convenience methods for testing operations
///
/// The methods within this trait are not recommended for use outside of testing contexts.
/// They are not intended for use in creating or managing production deployments, and
/// do not provide configurability generally required by those tools.
pub trait TestTableExt {
    /// Prepare a create table operation
    ///
    /// Table will be created with the primary key and index keys specified in _pay per request_
    /// mode.
    fn create_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::create_table::builders::CreateTableFluentBuilder;

    /// Prepare a delete table operation
    fn delete_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::delete_table::builders::DeleteTableFluentBuilder;
}

impl<T> TestTableExt for T
where
    T: Table,
{
    fn create_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::create_table::builders::CreateTableFluentBuilder {
        let definitions: std::collections::BTreeSet<_> =
            <<Self as Table>::IndexKeys as keys::IndexKeys>::KEY_DEFINITIONS
                .iter()
                .copied()
                .collect();

        let mut builder = self
            .client()
            .create_table()
            .set_table_name(Some(self.table_name().into()));

        for definition in definitions {
            let hash = aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .set_attribute_name(Some(definition.hash_key().into()))
                .set_attribute_type(Some(aws_sdk_dynamodb::types::ScalarAttributeType::S))
                .build()
                .expect("attribute name and attribute type are always provided");
            let mut key_schema = vec![aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .set_attribute_name(Some(definition.hash_key().into()))
                .set_key_type(Some(aws_sdk_dynamodb::types::KeyType::Hash))
                .build()
                .expect("attribute name and key type are always provided")];
            builder = builder.attribute_definitions(hash);
            if let Some(range_key) = definition.range_key() {
                let range = aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .set_attribute_name(Some(range_key.into()))
                    .set_attribute_type(Some(aws_sdk_dynamodb::types::ScalarAttributeType::S))
                    .build()
                    .expect("attribute name and attribute type are always provided");
                key_schema.push(
                    aws_sdk_dynamodb::types::KeySchemaElement::builder()
                        .set_attribute_name(Some(range_key.into()))
                        .set_key_type(Some(aws_sdk_dynamodb::types::KeyType::Range))
                        .build()
                        .expect("attribute name and key type are always provided"),
                );
                builder = builder.attribute_definitions(range)
            }
            let gsi = aws_sdk_dynamodb::types::GlobalSecondaryIndex::builder()
                .set_index_name(Some(definition.index_name().into()))
                .set_projection(Some(
                    aws_sdk_dynamodb::types::Projection::builder()
                        .set_projection_type(Some(aws_sdk_dynamodb::types::ProjectionType::All))
                        .build(),
                ))
                .set_key_schema(Some(key_schema))
                .build()
                .expect("index name and key schema are always provided");
            builder = builder.global_secondary_indexes(gsi);
        }

        let primary_key_definition =
            <<Self as Table>::PrimaryKey as keys::PrimaryKey>::PRIMARY_KEY_DEFINITION;
        let hash = aws_sdk_dynamodb::types::AttributeDefinition::builder()
            .set_attribute_name(Some(primary_key_definition.hash_key.into()))
            .set_attribute_type(Some(aws_sdk_dynamodb::types::ScalarAttributeType::S))
            .build()
            .expect("attribute name and attribute type are always provided");
        let mut key_schema = vec![aws_sdk_dynamodb::types::KeySchemaElement::builder()
            .set_attribute_name(Some(primary_key_definition.hash_key.into()))
            .set_key_type(Some(aws_sdk_dynamodb::types::KeyType::Hash))
            .build()
            .expect("attribute name and key type are always provided")];
        builder = builder.attribute_definitions(hash);
        if let Some(range_key) = primary_key_definition.range_key {
            let range = aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .set_attribute_name(Some(range_key.into()))
                .set_attribute_type(Some(aws_sdk_dynamodb::types::ScalarAttributeType::S))
                .build()
                .expect("attribute name and attribute type are always provided");
            key_schema.push(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .set_attribute_name(Some(range_key.into()))
                    .set_key_type(Some(aws_sdk_dynamodb::types::KeyType::Range))
                    .build()
                    .expect("attribute name and key type are always provided"),
            );
            builder = builder.attribute_definitions(range)
        }

        builder
            .set_key_schema(Some(key_schema))
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
    }

    fn delete_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::delete_table::builders::DeleteTableFluentBuilder {
        self.client()
            .delete_table()
            .set_table_name(Some(self.table_name().into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestTable;
    impl Table for TestTable {
        type PrimaryKey = keys::Primary;
        type IndexKeys = keys::Gsi13;

        fn client(&self) -> &aws_sdk_dynamodb::Client {
            unimplemented!()
        }

        fn table_name(&self) -> &str {
            unimplemented!()
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct TestEntity {
        id: String,
        name: String,
        email: String,
    }

    impl EntityDef for TestEntity {
        const ENTITY_TYPE: &'static EntityTypeNameRef = EntityTypeNameRef::from_static("test_ent");
    }

    impl Entity for TestEntity {
        type KeyInput<'a> = (&'a str, &'a str);
        type Table = TestTable;
        type IndexKeys = keys::Gsi13;

        fn primary_key((id, email): Self::KeyInput<'_>) -> keys::Primary {
            keys::Primary {
                hash: format!("PK#{id}"),
                range: format!("NAME#{email}"),
            }
        }

        fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
            keys::FullKey {
                primary: Self::primary_key((&self.id, &self.email)),
                indexes: keys::Gsi13 {
                    hash: format!("GSI13#{}", self.id),
                    range: format!("GSI13#NAME#{}", self.name),
                },
            }
        }
    }

    #[test]
    fn test_entity_serializes_as_expected() {
        let entity = TestEntity {
            id: "test1".to_string(),
            name: "Test".to_string(),
            email: "my_email@not_real.com".to_string(),
        };

        let item = entity.into_item();
        assert_eq!(item.len(), 8);
        assert_eq!(item["entity_type"].as_s().unwrap(), "test_ent");
        assert_eq!(item["PK"].as_s().unwrap(), "PK#test1");
        assert_eq!(item["SK"].as_s().unwrap(), "NAME#my_email@not_real.com");
        assert_eq!(item["GSI13PK"].as_s().unwrap(), "GSI13#test1");
        assert_eq!(item["GSI13SK"].as_s().unwrap(), "GSI13#NAME#Test");
        assert_eq!(item["id"].as_s().unwrap(), "test1");
        assert_eq!(item["name"].as_s().unwrap(), "Test");
        assert_eq!(item["email"].as_s().unwrap(), "my_email@not_real.com");
    }
}
