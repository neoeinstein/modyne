# Changelog

The format of this changelog is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

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
