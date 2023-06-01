# Modyne

_An opinionated library for interacting with AWS DynamoDB single-table designs._

[![docs.rs](https://img.shields.io/docsrs/modyne)][docsrs]
[![crates.io](https://img.shields.io/crates/v/modyne)][cratesio]
![MIT/Apache-2.0 dual licensed](https://img.shields.io/crates/l/modyne)
![modyne: rustc 1.70+](https://img.shields.io/badge/modyne-rustc_1.70+-lightgray.svg)†

## Motive

_Modyne_ follows the precepts laid out for effective single-table design when
working with DynamoDB, as laid out in [_The DynamoDB Book_ by Alex DeBrie][DDB]
and the [DynamoDB Guide companion website][DDG]. Such designs take advantage of
the fact that certain access patterns fit very well with the relatively simple
architecture of AWS DynamoDB, wherein a partition contains all the items for a
given _partition key_ within a BTree structure, sorted by a _sort key_.

Because of this architecture, DynamoDB rewards accessing ranges of items within
a single partition through the _Query_ operation. Additional access patterns may
be enabled through _global secondary indexes_, and sparse indexes can be used to
enable efficient _Scan_ operations.

Leveraging single-table design, multiple different entity types can be mixed
into a single DynamoDB table, and multiple entity types may cohabitate within an
individual partition. Often the raw attributes of an item are not used as the
keys. Instead, synthetic keys are generated from those raw attributes to support
the efficient use of partitions and indexes in Query and Scan operations.

## Concept

In _modyne_, the consumer of the crate will define their various entities in
code, providing implementations for Serde serialization and deserialization, as
well as providing an implementation for the `Entity` trait. This trait is the
core of _modyne_. It defines the name of the entity, what attributes matter to
the entity, and how the primary and any secondary index keys are generated for
the entity.

The next big concepts in this crate are the `QueryInput` and the
`ProjectionSet`. The query input makes it simple to define the key expression,
filter expressions, and the set of attributes required by the set of projections
to be read from the query. A dual to `QueryInput` exists for Scan operations:
`ScanInput`.

Beyond these concepts, _modyne_ provides higher-level fluent interfaces for
interacting with these DynamoDB entities that help guide usage toward the pit of
success and away from unintentional errors. The crate also provides fluent
interfaces for performing bulk and transactional read and write operations as
are sometimes required when keeping denormalized data in sync with a
single-table design.

For a more complete tour of the functionality in this crate, see the
[documentation on docs.rs][docsrs].

## Usage notes

While _modyne_ is available and ready for general use, you should be aware of a
few usage notes before you commit to this crate.

### Undocumented field usage

This library relies on the undocumented ability to directly access the `item`
field on response objects returned by the DynamoDB API. It does this because the
deserializer in `serde_dynamo` requires ownership of the items map. Without
directly accessing the field, every attempt to deserialize would require cloning
the entire items map every time, which would be undesirable and cause
significant performance overhead. In the event that this field becomes
unavailable, we may need to fall back to this less-performant mechanism.

### Binding to latest AWS SDK

As of now, this library will only bind itself to the latest version of the AWS
DynamoDB SDK. This may be updated in the future to use feature flags to allow
targeting multiple different SDK versions, but as this crate gets going we will
only target a single version of the AWS SDK, most often the latest version.

An update to the AWS SDK that does not break _modyne_'s usage of the AWS SDK
will only result in a minor version bump. If the AWS SDK exposes an observable
breaking change to _modyne_, then a major version bump will be used. Prior to
1.0, those bumps will be to the patch and minor version components,
respectively.

---

†: The MSRV for this crate can be lowered to 1.66.1 by enabling the
`once_cell` feature.

[cratesio]: https://crates.io/crates/modyne
[docsrs]: https://docs.rs/modyne
[DDB]: https://www.dynamodbbook.com/
[DDG]: https://www.dynamodbguide.com/
