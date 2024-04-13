//! Types useful as attributes in DynamoDB items

use std::time::SystemTime;

use time::{format_description::well_known::Rfc3339, OffsetDateTime};

/// A type representing the expiry (TTL) of a DynamoDB item
///
/// This type is used to represent the expiry of a DynamoDB item. It is
/// serialized as a Unix timestamp in seconds, as required to be used as
/// the TTL attribute of a DynamoDB item. To support range queries, the
/// timestamp may also be formatted in a standard, lexically sortable
/// format.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct Expiry {
    #[serde(with = "time::serde::timestamp")]
    inner: OffsetDateTime,
}

impl Expiry {
    /// Returns the expiry in RFC 3339 format, suitable for use as a component
    /// of a range key
    pub fn key_format(&self) -> String {
        self.inner.format(&Rfc3339).unwrap()
    }
}

impl From<OffsetDateTime> for Expiry {
    #[inline]
    fn from(ts: OffsetDateTime) -> Self {
        let inner = ts.to_offset(time::UtcOffset::UTC);
        let inner = inner.replace_nanosecond(0).unwrap();
        Self { inner }
    }
}

impl From<Expiry> for OffsetDateTime {
    #[inline]
    fn from(ts: Expiry) -> Self {
        ts.inner
    }
}

impl From<SystemTime> for Expiry {
    #[inline]
    fn from(ts: SystemTime) -> Self {
        OffsetDateTime::from(ts).into()
    }
}

impl From<Expiry> for SystemTime {
    #[inline]
    fn from(ts: Expiry) -> Self {
        OffsetDateTime::from(ts).into()
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::AttributeValue;

    use super::*;

    #[test]
    fn timestamp_matches_expected_format() {
        let ts: Expiry = OffsetDateTime::from_unix_timestamp(12345321)
            .unwrap()
            .into();
        assert_eq!(&ts.key_format(), "1970-05-23T21:15:21Z");
    }

    #[test]
    fn timestamp_removes_fractional_seconds() {
        let ts: Expiry = OffsetDateTime::parse("1970-05-23T21:15:21.012345678Z", &Rfc3339)
            .unwrap()
            .into();
        assert_eq!(&ts.key_format(), "1970-05-23T21:15:21Z");
    }

    #[test]
    fn timestamp_moves_to_utc() {
        let ts: Expiry = OffsetDateTime::parse("1970-05-23T21:15:21.012345678+03:30", &Rfc3339)
            .unwrap()
            .into();
        assert_eq!(&ts.key_format(), "1970-05-23T17:45:21Z");
    }

    #[test]
    fn timestamp_as_attribute_item_is_timestamp() {
        let ts: Expiry = OffsetDateTime::from_unix_timestamp(12345321)
            .unwrap()
            .into();
        let attribute = crate::codec::to_attribute_value(ts).unwrap();
        assert_eq!(attribute, AttributeValue::N("12345321".to_string()));
    }
}
