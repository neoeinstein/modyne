//! Types representing DynamoDB keys in a single-table design
//!
//! # Working with Local Secondary Indexes
//!
//! Because the partition key on an LSI be the same as the partition
//! key on the table, it _may_ be omitted when constructing the full set
//! of key attributes for a put or update operation. There is no danger
//! in including it, but it will be overriden by the table's partition
//! key.
//!
//! However, when used for a query or scan operation, the partition key
//! must be provided.
//!
//! # Example
//!
//! Constructing the key for an LSI as part of a put operation:
//!
//! ```
//! use modyne::keys;
//!
//! let primary = keys::Primary {
//!    hash: "PART#ABCD".to_string(),
//!    range: "SORT#1234".to_string(),
//! };
//! let lsi = keys::Lsi1 {
//!     hash: String::default(),
//!     range: "LSI1#9876".to_string(),
//! };
//! let full_key = keys::FullKey { primary, indexes: lsi }.into_key();
//!
//! assert_eq!(full_key["PK"].as_s().unwrap(), "PART#ABCD");
//! assert_eq!(full_key["SK"].as_s().unwrap(), "SORT#1234");
//! assert_eq!(full_key["LSI1SK"].as_s().unwrap(), "LSI1#9876");
//! ```
//!
//! Constructing the key for an LSI as part of a query operation:
//!
//! ```
//! use modyne::keys::{IndexKeys, Lsi1};
//!
//! let lsi = Lsi1 {
//!     hash: "PART#ABCD".to_string(),
//!     range: "LSI1#9876".to_string(),
//! };
//! let full_key = lsi.into_key();
//!
//! assert_eq!(full_key["PK"].as_s().unwrap(), "PART#ABCD");
//! assert_eq!(full_key["LSI1SK"].as_s().unwrap(), "LSI1#9876");
//! ```

use crate::Item;

/// A DynamoDB key
pub trait Key: Sized + serde::Serialize {
    /// The core properties of the key, determining how data is stored and accessed
    const DEFINITION: KeyDefinition;
}

/// A set of keys used as secondary indexes
pub trait IndexKeys: Sized {
    /// The definitions for the keys
    const KEY_DEFINITIONS: &'static [SecondaryIndexDefinition];

    /// The intermediate type used to serialize the key
    type Serialize<'a>: serde::Serialize
    where
        Self: 'a;

    /// Constructs the intermediate type used to serialize the key
    fn to_serialize(&self) -> Self::Serialize<'_>;

    /// Converts the key into a DynamoDB item
    fn into_key(self) -> Item {
        crate::codec::to_item(self.to_serialize()).unwrap()
    }
}

/// A DynamoDB primary key
pub trait PrimaryKey: Sized + serde::Serialize {
    /// The definition for the primary key
    const PRIMARY_KEY_DEFINITION: PrimaryKeyDefinition;

    /// Converts the key into a DynamoDB item
    fn into_key(self) -> Item {
        crate::codec::to_item(self).unwrap()
    }
}

/// The primary key for a DynamoDB table
#[derive(Clone, Debug, serde::Serialize)]
pub struct Primary {
    /// The partition key, with attribute name `PK`
    #[serde(rename = "PK")]
    pub hash: String,

    /// The sort key, with attribute name `SK`
    #[serde(rename = "SK")]
    pub range: String,
}

impl PrimaryKey for Primary {
    const PRIMARY_KEY_DEFINITION: PrimaryKeyDefinition = PrimaryKeyDefinition {
        hash_key: "PK",
        range_key: Some("SK"),
    };
}

impl Key for Primary {
    const DEFINITION: KeyDefinition = KeyDefinition::Primary(Self::PRIMARY_KEY_DEFINITION);
}

/// A DynamoDB secondary index key
pub trait IndexKey: Sized + serde::Serialize {
    /// The definition for the index
    const INDEX_DEFINITION: SecondaryIndexDefinition;
}

impl<K: IndexKey> Key for K {
    const DEFINITION: KeyDefinition = KeyDefinition::Secondary(K::INDEX_DEFINITION);
}

impl<K: IndexKey> IndexKey for Option<K> {
    const INDEX_DEFINITION: SecondaryIndexDefinition = K::INDEX_DEFINITION;
}

/// The primary key for an item along with the relevant secondary index keys
#[derive(Clone, Debug, serde::Serialize)]
pub struct FullKey<P, I>
where
    P: PrimaryKey,
    I: IndexKeys,
{
    /// The secondary index keys relavant to the item
    #[serde(flatten, serialize_with = "serialize_keys")]
    pub indexes: I,

    /// The primary key for the item
    #[serde(flatten)]
    pub primary: P,
}

impl<P, I> FullKey<P, I>
where
    P: PrimaryKey,
    I: IndexKeys,
{
    /// Converts the key into a DynamoDB item
    pub fn into_key(self) -> Item {
        crate::codec::to_item(self).unwrap()
    }
}

impl<P> From<P> for FullKey<P, ()>
where
    P: PrimaryKey,
{
    #[inline]
    fn from(primary: P) -> Self {
        Self {
            indexes: (),
            primary,
        }
    }
}

fn serialize_keys<K, S>(keys: &K, serializer: S) -> Result<S::Ok, S::Error>
where
    K: IndexKeys,
    S: serde::Serializer,
{
    serde::Serialize::serialize(&keys.to_serialize(), serializer)
}

macro_rules! gsi_key {
    ($name:ident: $idx:literal, $pk:literal, $sk:literal) => {
        /// The key for a global secondary index
        #[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, serde::Serialize)]
        pub struct $name {
            #[doc = "The partition key, with attribute name `"]
            #[doc = $pk]
            #[doc = "`"]
            #[serde(rename = $pk)]
            pub hash: String,

            #[doc = "The sort key, with attribute name `"]
            #[doc = $pk]
            #[doc = "`"]
            #[serde(rename = $sk)]
            pub range: String,
        }

        impl IndexKey for $name {
            const INDEX_DEFINITION: SecondaryIndexDefinition =
                SecondaryIndexDefinition::Global(GlobalSecondaryIndexDefinition {
                    index_name: $idx,
                    hash_key: $pk,
                    range_key: Some($sk),
                });
        }
    };
}

gsi_key!(Gsi1: "GSI1", "GSI1PK", "GSI1SK");
gsi_key!(Gsi2: "GSI2", "GSI2PK", "GSI2SK");
gsi_key!(Gsi3: "GSI3", "GSI3PK", "GSI3SK");
gsi_key!(Gsi4: "GSI4", "GSI4PK", "GSI4SK");
gsi_key!(Gsi5: "GSI5", "GSI5PK", "GSI5SK");
gsi_key!(Gsi6: "GSI6", "GSI6PK", "GSI6SK");
gsi_key!(Gsi7: "GSI7", "GSI7PK", "GSI7SK");
gsi_key!(Gsi8: "GSI8", "GSI8PK", "GSI8SK");
gsi_key!(Gsi9: "GSI9", "GSI9PK", "GSI9SK");
gsi_key!(Gsi10: "GSI10", "GSI10PK", "GSI10SK");
gsi_key!(Gsi11: "GSI11", "GSI11PK", "GSI11SK");
gsi_key!(Gsi12: "GSI12", "GSI12PK", "GSI12SK");
gsi_key!(Gsi13: "GSI13", "GSI13PK", "GSI13SK");
gsi_key!(Gsi14: "GSI14", "GSI14PK", "GSI14SK");
gsi_key!(Gsi15: "GSI15", "GSI15PK", "GSI15SK");
gsi_key!(Gsi16: "GSI16", "GSI16PK", "GSI16SK");
gsi_key!(Gsi17: "GSI17", "GSI17PK", "GSI17SK");
gsi_key!(Gsi18: "GSI18", "GSI18PK", "GSI18SK");
gsi_key!(Gsi19: "GSI19", "GSI19PK", "GSI19SK");
gsi_key!(Gsi20: "GSI20", "GSI20PK", "GSI20SK");

macro_rules! lsi_key {
    ($name:ident: $idx:literal, $sk:literal) => {
        /// The key for a local secondary index
        ///
        /// See the [module documentation][crate::keys#Working_with_Local_Secondary_Indexes]
        /// for more information on how to use this type.
        #[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, serde::Serialize)]
        pub struct $name {
            /// The partition key for the table, with attribute name `PK`
            #[serde(rename = "PK")]
            pub hash: String,

            #[doc = "The sort key for the local secondary index, with attribute name `"]
            #[doc = $sk]
            #[doc = "`"]
            #[serde(rename = $sk)]
            pub range: String,
        }

        impl IndexKey for $name {
            const INDEX_DEFINITION: SecondaryIndexDefinition =
                SecondaryIndexDefinition::Local(LocalSecondaryIndexDefinition {
                    index_name: $idx,
                    hash_key: "PK",
                    range_key: $sk,
                });
        }
    };
}

lsi_key!(Lsi1: "LSI1", "LSI1SK");
lsi_key!(Lsi2: "LSI2", "LSI2SK");
lsi_key!(Lsi3: "LSI3", "LSI3SK");
lsi_key!(Lsi4: "LSI4", "LSI4SK");
lsi_key!(Lsi5: "LSI5", "LSI5SK");

macro_rules! impl_key_tuples {
    ($i:ident; $($n:tt : $ty:ident),*$(,)?) => {
        /// A composite serialization of multiple keys
        #[derive(Debug, serde::Serialize)]
        #[allow(non_snake_case)]
        pub struct $i<'a, $($ty),*> {
            $(#[serde(flatten)] $ty: &'a $ty,)*
        }

        impl<$($ty: IndexKey),*> IndexKeys for ($($ty,)*)
        where
            $(
                for<'a> $ty: 'a,
            )*
        {
            const KEY_DEFINITIONS: &'static [$crate::keys::SecondaryIndexDefinition] = &[
                $(
                    $ty::INDEX_DEFINITION,
                )*
            ];
            type Serialize<'a> = $i<'a, $($ty),*>;
            #[inline]
            fn to_serialize(&self) -> Self::Serialize<'_> {
                $i {
                    $($ty: &self.$n,)*
                }
            }
        }
    };
}

impl<T: IndexKey> IndexKeys for T {
    const KEY_DEFINITIONS: &'static [SecondaryIndexDefinition] = &[T::INDEX_DEFINITION];
    type Serialize<'a>
        = &'a T
    where
        T: 'a;
    #[inline]
    fn to_serialize(&self) -> Self::Serialize<'_> {
        self
    }
}

impl<K: Key> crate::ScanInput for K {
    type Index = K;
}

mod hidden {
    #[derive(Debug, serde::Serialize)]
    pub struct Empty {}
}

impl IndexKeys for () {
    const KEY_DEFINITIONS: &'static [SecondaryIndexDefinition] = &[];
    type Serialize<'a> = hidden::Empty;
    #[inline]
    fn to_serialize(&self) -> Self::Serialize<'_> {
        hidden::Empty {}
    }
}

mod composite_keys {
    use super::*;
    impl_key_tuples! { CompositeK0; 0: K0 }
    impl_key_tuples! { CompositeK1; 0: K0, 1: K1 }
    impl_key_tuples! { CompositeK2; 0: K0, 1: K1, 2: K2 }
    impl_key_tuples! { CompositeK3; 0: K0, 1: K1, 2: K2, 3: K3 }
    impl_key_tuples! { CompositeK4; 0: K0, 1: K1, 2: K2, 3: K3, 4: K4 }
    impl_key_tuples! { CompositeK5; 0: K0, 1: K1, 2: K2, 3: K3, 4: K4, 5: K5 }
    impl_key_tuples! { CompositeK6; 0: K0, 1: K1, 2: K2, 3: K3, 4: K4, 5: K5, 6: K6 }
    impl_key_tuples! { CompositeK7; 0: K0, 1: K1, 2: K2, 3: K3, 4: K4, 5: K5, 6: K6, 7: K7 }
    impl_key_tuples! { CompositeK8; 0: K0, 1: K1, 2: K2, 3: K3, 4: K4, 5: K5, 6: K6, 7: K7, 8: K8 }
    impl_key_tuples! { CompositeK9; 0: K0, 1: K1, 2: K2, 3: K3, 4: K4, 5: K5, 6: K6, 7: K7, 8: K8, 9: K9 }
    impl_key_tuples! { CompositeK10; 0: K0, 1: K1, 2: K2, 3: K3, 4: K4, 5: K5, 6: K6, 7: K7, 8: K8, 9: K9, 10: K10 }
    impl_key_tuples! { CompositeK11; 0: K0, 1: K1, 2: K2, 3: K3, 4: K4, 5: K5, 6: K6, 7: K7, 8: K8, 9: K9, 10: K10, 11: K11 }
    impl_key_tuples! { CompositeK12; 0: K0, 1: K1, 2: K2, 3: K3, 4: K4, 5: K5, 6: K6, 7: K7, 8: K8, 9: K9, 10: K10, 11: K11, 12: K12 }
}

/// A key definition
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub enum KeyDefinition {
    /// The primary key
    Primary(PrimaryKeyDefinition),

    /// A secondary index
    Secondary(SecondaryIndexDefinition),
}

impl KeyDefinition {
    /// The name of the index, if any
    #[inline]
    pub const fn index_name(&self) -> Option<&'static str> {
        match self {
            Self::Primary(_) => None,
            Self::Secondary(def) => Some(def.index_name()),
        }
    }

    /// The hash key
    #[inline]
    pub const fn hash_key(&self) -> &'static str {
        match self {
            Self::Primary(def) => def.hash_key,
            Self::Secondary(def) => def.hash_key(),
        }
    }

    /// The range key, if any
    #[inline]
    pub const fn range_key(&self) -> Option<&'static str> {
        match self {
            Self::Primary(def) => def.range_key,
            Self::Secondary(def) => def.range_key(),
        }
    }
}

impl From<PrimaryKeyDefinition> for KeyDefinition {
    #[inline]
    fn from(def: PrimaryKeyDefinition) -> Self {
        Self::Primary(def)
    }
}

impl From<SecondaryIndexDefinition> for KeyDefinition {
    #[inline]
    fn from(def: SecondaryIndexDefinition) -> Self {
        Self::Secondary(def)
    }
}

/// A primary key definition
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct PrimaryKeyDefinition {
    /// The hash key
    pub hash_key: &'static str,

    /// The range key, if any
    pub range_key: Option<&'static str>,
}

impl PrimaryKeyDefinition {
    /// Convert into a key definition
    #[inline]
    pub const fn into_key_definition(self) -> KeyDefinition {
        KeyDefinition::Primary(self)
    }
}

/// A secondary index definition
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub enum SecondaryIndexDefinition {
    /// A global secondary index
    Global(GlobalSecondaryIndexDefinition),

    /// A local secondary index
    Local(LocalSecondaryIndexDefinition),
}

impl SecondaryIndexDefinition {
    /// Get the name of the index
    #[inline]
    pub const fn index_name(&self) -> &'static str {
        match self {
            Self::Global(def) => def.index_name,
            Self::Local(def) => def.index_name,
        }
    }

    /// Get the hash key of the index
    #[inline]
    pub const fn hash_key(&self) -> &'static str {
        match self {
            Self::Global(def) => def.hash_key,
            Self::Local(def) => def.hash_key,
        }
    }

    /// Get the range key of the index
    #[inline]
    pub const fn range_key(&self) -> Option<&'static str> {
        match self {
            Self::Global(def) => def.range_key,
            Self::Local(def) => Some(def.range_key),
        }
    }

    /// Convert into a key definition
    #[inline]
    pub const fn into_key_definition(self) -> KeyDefinition {
        KeyDefinition::Secondary(self)
    }
}

/// A global secondary index definition
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct GlobalSecondaryIndexDefinition {
    /// The name of the index
    pub index_name: &'static str,

    /// The hash key of the index
    pub hash_key: &'static str,

    /// The range key of the index
    pub range_key: Option<&'static str>,
}

/// A global secondary index definition
impl GlobalSecondaryIndexDefinition {
    /// Convert into a secondary index definition
    #[inline]
    pub const fn into_index(self) -> SecondaryIndexDefinition {
        SecondaryIndexDefinition::Global(self)
    }
}

/// A local secondary index definition
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct LocalSecondaryIndexDefinition {
    /// The name of the index
    pub index_name: &'static str,

    /// The hash key of the table
    ///
    /// This must match the name of the hash key of the table
    pub hash_key: &'static str,

    /// The range key of the index
    pub range_key: &'static str,
}

/// A local secondary index definition
impl LocalSecondaryIndexDefinition {
    /// Convert into a secondary index definition
    #[inline]
    pub const fn into_index(self) -> SecondaryIndexDefinition {
        SecondaryIndexDefinition::Local(self)
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::AttributeValue;

    use super::*;

    #[test]
    fn test_primary_key() {
        let key = Primary {
            hash: "hash".to_string(),
            range: "range".to_string(),
        };
        let serialized = key.into_key();
        assert_eq!(serialized["PK"], AttributeValue::S("hash".to_string()));
        assert_eq!(serialized["SK"], AttributeValue::S("range".to_string()));
    }

    #[test]
    fn test_gsi_key() {
        let key = Gsi1 {
            hash: "hash".to_string(),
            range: "range".to_string(),
        };
        let serialized = key.into_key();
        assert_eq!(serialized["GSI1PK"], AttributeValue::S("hash".to_string()));
        assert_eq!(serialized["GSI1SK"], AttributeValue::S("range".to_string()));
    }

    #[test]
    fn test_lsi_key() {
        let key = Lsi1 {
            hash: "primary_key".to_string(),
            range: "range".to_string(),
        };
        let serialized = key.into_key();
        assert_eq!(
            serialized["PK"],
            AttributeValue::S("primary_key".to_string())
        );
        assert_eq!(serialized["LSI1SK"], AttributeValue::S("range".to_string()));
    }

    #[test]
    fn test_composite_key() {
        let primary = Primary {
            hash: "PK".to_string(),
            range: "SK".to_string(),
        };

        let gsi5 = Gsi5 {
            hash: "GSI5PK".to_string(),
            range: "GSI5SK".to_string(),
        };

        let lsi3 = Lsi3 {
            // Note that this _should_ be the same as the primary key's hash, but
            // we set it to something else to make sure it is overridden once
            // serialized.
            hash: "LSI3PK".to_string(),
            range: "LSI3SK".to_string(),
        };

        let serialized = FullKey {
            primary,
            indexes: (gsi5, lsi3),
        }
        .into_key();
        assert_eq!(serialized["PK"], AttributeValue::S("PK".to_string()));
        assert_eq!(serialized["SK"], AttributeValue::S("SK".to_string()));
        assert_eq!(
            serialized["GSI5PK"],
            AttributeValue::S("GSI5PK".to_string())
        );
        assert_eq!(
            serialized["GSI5SK"],
            AttributeValue::S("GSI5SK".to_string())
        );
        assert_eq!(
            serialized["LSI3SK"],
            AttributeValue::S("LSI3SK".to_string())
        );
    }
}
