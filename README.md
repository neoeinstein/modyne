# Modyne

_An opinionated library for interacting with AWS DynamoDB single-table designs._

[![docs.rs](https://img.shields.io/docsrs/modyne)][docsrs]
[![crates.io](https://img.shields.io/crates/v/modyne)][cratesio]
![MIT/Apache-2.0 dual licensed](https://img.shields.io/crates/l/modyne)
![modyne: rustc 1.71+](https://img.shields.io/badge/modyne-rustc_1.71+-lightgray.svg)

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

---

[cratesio]: https://crates.io/crates/modyne
[docsrs]: https://docs.rs/modyne
[DDB]: https://www.dynamodbbook.com/
[DDG]: https://www.dynamodbguide.com/
