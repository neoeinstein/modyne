# Changelog

The format of this changelog is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [0.2.0] - 2023-09-22

_Note_: Due to the updated MSRV of the `time` crate, Modyne has updated its MSRV to 1.67.0.

- BREAKING: Updated AWS SDK to 0.56, DynamoDB SDK to 0.30 ([#5])
- Fix: `Update` operation did not include update expression names and values ([#4])

[#4]: https://github.com/neoeinstein/modyne/pull/4
[#5]: https://github.com/neoeinstein/modyne/pull/5

## [0.1.0] - 2023-06-01

- Initial release
