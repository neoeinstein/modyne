# Changelog

The format of this changelog is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.3.1](https://github.com/neoeinstein/modyne/compare/modyne-v0.3.0...modyne-v0.3.1) (2026-02-05)


### Bug Fixes

* bump MSRV ([#93](https://github.com/neoeinstein/modyne/issues/93)) ([5a914b5](https://github.com/neoeinstein/modyne/commit/5a914b50a100cf386dfd47b5687e4fe7f9d2ed72))
* commit for versioning ([340e66c](https://github.com/neoeinstein/modyne/commit/340e66c3bce5c2f2e5777ce4472954b9b09260a7))
* **deps:** update rust crate thiserror to v2 ([#148](https://github.com/neoeinstein/modyne/issues/148)) ([5559508](https://github.com/neoeinstein/modyne/commit/5559508788dcc242977e973195c310d37b21f69c))
* un-pin modyne-derive ([32b1cdb](https://github.com/neoeinstein/modyne/commit/32b1cdb05324764849661de4c0eb163e71a87a4a))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * modyne-derive bumped from 0.3 to 0.3.1
    * modyne-derive bumped from =0.3.0 to 0.3.1

## [Unreleased]

## [0.3.0] - 2023-12-07

_Note_: Due to the updated MSRV of the AWS SDK, Modyne has updated its MSRV to 1.68.0

- BREAKING: Updated the AWS SDK to 1.0 ([#13])
- New: Added support for non-standard entity type attribute names and values ([#15])

[#13](https://github.com/neoeinstein/modyne/issues/13)
[#15](https://github.com/neoeinstein/modyne/issues/15)

## [0.2.1] - 2023-11-15

- Fix: Correctly handle `KeyCondition`s that don't specify a sort key ([#9])
- Fix: Correct the span kind assigned to client DynamoDB spans ([#10])

[#9]: https://github.com/neoeinstein/modyne/pull/9
[#10]: https://github.com/neoeinstein/modyne/pull/10

## [0.2.0] - 2023-09-22

_Note_: Due to the updated MSRV of the `time` crate, Modyne has updated its MSRV to 1.67.0.

- BREAKING: Updated AWS SDK to 0.56, DynamoDB SDK to 0.30 ([#5])
- Fix: `Update` operation did not include update expression names and values ([#4])

[#4]: https://github.com/neoeinstein/modyne/pull/4
[#5]: https://github.com/neoeinstein/modyne/pull/5

## [0.1.0] - 2023-06-01

- Initial release
