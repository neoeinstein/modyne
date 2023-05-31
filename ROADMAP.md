# Roadmap for Modyne

* [x] Derive macro for entity types, wrapping Serde interaction
* [ ] Increase the type safety of the non-bulk read interactions with DynamoDB (GetItem, Query, Scan)
* [ ] Consider providing a `prelude` module with access to common extension traits
* [ ] Consider re-exposing the specific `aws_sdk_dynamodb` and `serde_dynamo` crates depended on
* [ ] Allow targeting multiple versions of the AWS SDK using feature flags
* [ ] Support usage with types from DynamoDB Streams
