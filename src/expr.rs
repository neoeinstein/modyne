//! Expression builders

use std::{fmt, marker::PhantomData};

use aws_sdk_dynamodb::types::AttributeValue;
use fnv::FnvHashSet;

use crate::keys;

/// A builder for a key condition expression, used in query operations
#[must_use]
pub struct KeyCondition<K> {
    partition_key: AttributeValue,
    sort_key: Option<SortKeyCondition>,
    key_type: PhantomData<fn() -> K>,
}

impl<K> fmt::Debug for KeyCondition<K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("KeyCondition")
            .field("key_type", &std::any::type_name::<K>())
            .field("partition_key", &self.partition_key)
            .field("sort_key", &self.sort_key)
            .finish()
    }
}

impl<K> Clone for KeyCondition<K> {
    fn clone(&self) -> Self {
        Self {
            key_type: PhantomData,
            partition_key: self.partition_key.clone(),
            sort_key: self.sort_key.clone(),
        }
    }
}

const PARTITION_KEY_EXPRESSION: &str = "#key_PK = :key_PK";
const PARTITION_EQ_KEY_EXPRESSION: &str = "#key_PK = :key_PK AND #key_SK = :key_SK";
const PARTITION_BETWEEN_KEY_EXPRESSION: &str =
    "#key_PK = :key_PK AND #key_SK BETWEEN :key_SK_START AND :key_SK_END";
const PARTITION_LT_KEY_EXPRESSION: &str = "#key_PK = :key_PK AND #key_SK < :key_SK";
const PARTITION_LTE_KEY_EXPRESSION: &str = "#key_PK = :key_PK AND #key_SK <= :key_SK";
const PARTITION_GT_KEY_EXPRESSION: &str = "#key_PK = :key_PK AND #key_SK > :key_SK";
const PARTITION_GTE_KEY_EXPRESSION: &str = "#key_PK = :key_PK AND #key_SK >= :key_SK";
const PARTITION_BEGINS_WITH_KEY_EXPRESSION: &str =
    "#key_PK = :key_PK AND begins_with(#key_SK, :key_SK)";

impl<K> KeyCondition<K>
where
    K: keys::Key,
{
    /// Get items in the given partition
    ///
    /// # Panics
    ///
    /// Panics if the partition cannot be serialized to an `AttributeValue`.
    pub fn in_partition<V: serde::Serialize>(partition: V) -> Self {
        KeyCondition {
            partition_key: serde_dynamo::to_attribute_value(partition).unwrap(),
            sort_key: None,
            key_type: PhantomData,
        }
    }

    /// Get the item where the sort key is equal to the given value
    ///
    /// # Panics
    ///
    /// Panics if the given value cannot be serialized to an `AttributeValue`.
    pub fn specific_item<V: serde::Serialize>(mut self, sort: V) -> Self {
        Self::ensure_range_key();
        self.sort_key = Some(SortKeyCondition::Equal(
            serde_dynamo::to_attribute_value(sort).unwrap(),
        ));
        self
    }

    /// Get items where the sort key is in a range between the start and end values, inclusive
    ///
    /// # Panics
    ///
    /// Panics if either of the given values cannot be serialized to an `AttributeValue`.
    pub fn between<V: serde::Serialize>(mut self, start: V, end: V) -> Self {
        Self::ensure_range_key();
        self.sort_key = Some(SortKeyCondition::Between {
            start: serde_dynamo::to_attribute_value(start).unwrap(),
            end: serde_dynamo::to_attribute_value(end).unwrap(),
        });
        self
    }

    /// Get items where the sort key is less than the given value
    ///
    /// # Panics
    ///
    /// Panics if the given value cannot be serialized to an `AttributeValue`.
    pub fn less_than<V: serde::Serialize>(mut self, sort: V) -> Self {
        Self::ensure_range_key();
        self.sort_key = Some(SortKeyCondition::LessThan(
            serde_dynamo::to_attribute_value(sort).unwrap(),
        ));
        self
    }

    /// Get items where the sort key is less than or equal to the given value
    ///
    /// # Panics
    ///
    /// Panics if the given value cannot be serialized to an `AttributeValue`.
    pub fn less_than_or_equal<V: serde::Serialize>(mut self, sort: V) -> Self {
        Self::ensure_range_key();
        self.sort_key = Some(SortKeyCondition::LessThanOrEqual(
            serde_dynamo::to_attribute_value(sort).unwrap(),
        ));
        self
    }

    /// Get items where the sort key is greater than the given value
    ///
    /// # Panics
    ///
    /// Panics if the given value cannot be serialized to an `AttributeValue`.
    pub fn greater_than<V: serde::Serialize>(mut self, sort: V) -> Self {
        Self::ensure_range_key();
        self.sort_key = Some(SortKeyCondition::GreaterThan(
            serde_dynamo::to_attribute_value(sort).unwrap(),
        ));
        self
    }

    /// Get items where the sort key is greater than or equal to the given value
    ///
    /// # Panics
    ///
    /// Panics if the given value cannot be serialized to an `AttributeValue`.
    pub fn greater_than_or_equal<V: serde::Serialize>(mut self, sort: V) -> Self {
        Self::ensure_range_key();
        self.sort_key = Some(SortKeyCondition::GreaterThanOrEqual(
            serde_dynamo::to_attribute_value(sort).unwrap(),
        ));
        self
    }

    /// Get items where the sort key begins with the given value
    pub fn begins_with(mut self, sort: impl Into<String>) -> Self {
        Self::ensure_range_key();
        self.sort_key = Some(SortKeyCondition::BeginsWith(sort.into()));
        self
    }

    #[inline]
    fn ensure_range_key() {
        if let Some(idx) = K::DEFINITION.index_name() {
            assert!(
                K::DEFINITION.range_key().is_some(),
                "Key on index `{idx}` does not have a range key",
            )
        } else {
            assert!(
                K::DEFINITION.range_key().is_some(),
                "Primary key does not have a range key",
            )
        }
    }

    pub(crate) fn expression(&self) -> &'static str {
        match &self.sort_key {
            Some(SortKeyCondition::Equal(_)) => PARTITION_EQ_KEY_EXPRESSION,
            Some(SortKeyCondition::Between { .. }) => PARTITION_BETWEEN_KEY_EXPRESSION,
            Some(SortKeyCondition::LessThan(_)) => PARTITION_LT_KEY_EXPRESSION,
            Some(SortKeyCondition::LessThanOrEqual(_)) => PARTITION_LTE_KEY_EXPRESSION,
            Some(SortKeyCondition::GreaterThan(_)) => PARTITION_GT_KEY_EXPRESSION,
            Some(SortKeyCondition::GreaterThanOrEqual(_)) => PARTITION_GTE_KEY_EXPRESSION,
            Some(SortKeyCondition::BeginsWith(_)) => PARTITION_BEGINS_WITH_KEY_EXPRESSION,
            None => PARTITION_KEY_EXPRESSION,
        }
    }

    pub(crate) fn names(&self) -> impl Iterator<Item = (&'static str, &'static str)> {
        let names = if let Some(sk) = K::DEFINITION.range_key() {
            [
                Some(("#key_PK", K::DEFINITION.hash_key())),
                Some(("#key_SK", sk)),
            ]
        } else {
            [Some(("#key_PK", K::DEFINITION.hash_key())), None]
        };
        names.into_iter().flatten()
    }

    pub(crate) fn values(self) -> impl Iterator<Item = (&'static str, AttributeValue)> {
        let values = if K::DEFINITION.range_key().is_some() {
            match self.sort_key {
                Some(SortKeyCondition::Between { start, end }) => [
                    Some((":key_PK", self.partition_key)),
                    Some((":key_SK_START", start)),
                    Some((":key_SK_END", end)),
                ],
                Some(
                    SortKeyCondition::Equal(v)
                    | SortKeyCondition::LessThan(v)
                    | SortKeyCondition::LessThanOrEqual(v)
                    | SortKeyCondition::GreaterThan(v)
                    | SortKeyCondition::GreaterThanOrEqual(v),
                ) => [
                    Some((":key_PK", self.partition_key)),
                    Some((":key_SK", v)),
                    None,
                ],
                Some(SortKeyCondition::BeginsWith(prefix)) => [
                    Some((":key_PK", self.partition_key)),
                    Some((":key_SK", AttributeValue::S(prefix))),
                    None,
                ],
                None => [Some((":key_PK", self.partition_key)), None, None],
            }
        } else {
            [Some((":key_PK", self.partition_key)), None, None]
        };

        values.into_iter().flatten()
    }
}

#[derive(Debug, Clone)]
#[must_use]
enum SortKeyCondition {
    Equal(AttributeValue),
    Between {
        start: AttributeValue,
        end: AttributeValue,
    },
    LessThan(AttributeValue),
    LessThanOrEqual(AttributeValue),
    GreaterThan(AttributeValue),
    GreaterThanOrEqual(AttributeValue),
    BeginsWith(String),
}

/// A compiled filter expression
#[must_use]
#[derive(Clone)]
pub struct Filter {
    /// The parameterized expression
    pub expression: String,

    /// The attribute names used in the expression
    pub names: Vec<(String, String)>,

    /// The attribute values used in the expression
    pub values: Vec<(String, AttributeValue)>,

    /// The sensitive attribute values used in the expression that should not be logged
    pub sensitive_values: Vec<(String, AttributeValue)>,
}

impl Filter {
    /// Create a new filter expression
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression
                .into()
                .replace('#', "#flt_")
                .replace(':', ":flt_"),
            names: Vec::new(),
            values: Vec::new(),
            sensitive_values: Vec::new(),
        }
    }

    /// Add a name to the expression
    pub fn name(mut self, name: &str, value: impl Into<String>) -> Self {
        let name = format!("#flt_{}", name.trim_start_matches('#'));
        self.names.push((name, value.into()));
        self
    }

    /// Add a value to the expression
    ///
    /// # Panics
    ///
    /// Panics if the given value cannot be serialized to an `AttributeValue`.
    pub fn value(mut self, name: &str, value: impl serde::Serialize) -> Self {
        let name = format!(":flt_{}", name.trim_start_matches(':'));
        let value = serde_dynamo::to_attribute_value(value).unwrap();
        self.values.push((name, value));
        self
    }

    /// Add a sensitive value to the expression
    ///
    /// # Panics
    ///
    /// Panics if the given value cannot be serialized to an `AttributeValue`.
    pub fn sensitive_value(mut self, name: &str, value: impl serde::Serialize) -> Self {
        let name = format!(":flt_{}", name.trim_start_matches(':'));
        let value = serde_dynamo::to_attribute_value(value).unwrap();
        self.sensitive_values.push((name, value));
        self
    }
}

impl fmt::Debug for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Filter")
            .field("expression", &self.expression)
            .field("names", &self.names)
            .field("values", &self.values)
            .field(
                "sensitive_values",
                &format_args!("<{} values>", self.sensitive_values.len()),
            )
            .finish()
    }
}

/// A compiled update expression
#[derive(Clone)]
#[must_use]
pub struct Update {
    /// The parameterized expression
    pub expression: String,

    /// The attribute names used in the expression
    pub names: Vec<(String, String)>,

    /// The attribute values used in the expression
    pub values: Vec<(String, AttributeValue)>,

    /// The sensitive attribute values used in the expression that should not be logged
    pub sensitive_values: Vec<(String, AttributeValue)>,
}

impl Update {
    /// Create a new update expression
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression
                .into()
                .replace('#', "#upd_")
                .replace(':', ":upd_"),
            names: Vec::new(),
            values: Vec::new(),
            sensitive_values: Vec::new(),
        }
    }

    /// Add a name to the expression
    pub fn name(mut self, name: &str, value: impl Into<String>) -> Self {
        let name = format!("#upd_{}", name.trim_start_matches('#'));
        self.names.push((name, value.into()));
        self
    }

    /// Add a value to the expression
    ///
    /// # Panics
    ///
    /// Panics if the given value cannot be serialized to an `AttributeValue`.
    pub fn value(mut self, name: &str, value: impl serde::Serialize) -> Self {
        let name = format!(":upd_{}", name.trim_start_matches(':'));
        let value = serde_dynamo::to_attribute_value(value).unwrap();
        self.values.push((name, value));
        self
    }

    /// Add a sensitive value to the expression
    ///
    /// # Panics
    ///
    /// Panics if the given value cannot be serialized to an `AttributeValue`.
    pub fn sensitive_value(mut self, name: &str, value: impl serde::Serialize) -> Self {
        let name = format!(":upd_{}", name.trim_start_matches(':'));
        let value = serde_dynamo::to_attribute_value(value).unwrap();
        self.sensitive_values.push((name, value));
        self
    }
}

impl fmt::Debug for Update {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Update")
            .field("expression", &self.expression)
            .field("names", &self.names)
            .field("values", &self.values)
            .field(
                "sensitive_values",
                &format_args!("<{} values>", self.sensitive_values.len()),
            )
            .finish()
    }
}

#[derive(Clone)]
#[must_use]
/// A compiled condition expression
pub struct Condition {
    /// The parameterized expression
    pub expression: String,

    /// The attribute names used in the expression
    pub names: Vec<(String, String)>,

    /// The attribute values used in the expression
    pub values: Vec<(String, AttributeValue)>,

    /// The sensitive attribute values used in the expression that should not be logged
    pub sensitive_values: Vec<(String, AttributeValue)>,
}

impl Condition {
    /// Create a new condition expression
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression
                .into()
                .replace('#', "#cnd_")
                .replace(':', ":cnd_"),
            names: Vec::new(),
            values: Vec::new(),
            sensitive_values: Vec::new(),
        }
    }

    /// Add a name to the expression
    pub fn name(mut self, name: &str, value: impl Into<String>) -> Self {
        let name = format!("#cnd_{}", name.trim_start_matches('#'));
        self.names.push((name, value.into()));
        self
    }

    /// Add a value to the expression
    ///
    /// # Panics
    ///
    /// Panics if the given value cannot be serialized to an `AttributeValue`.
    pub fn value(mut self, name: &str, value: impl serde::Serialize) -> Self {
        let name = format!(":cnd_{}", name.trim_start_matches(':'));
        let value = serde_dynamo::to_attribute_value(value).unwrap();
        self.values.push((name, value));
        self
    }

    /// Add a sensitive value to the expression
    ///
    /// # Panics
    ///
    /// Panics if the given value cannot be serialized to an `AttributeValue`.
    pub fn sensitive_value(mut self, name: &str, value: impl serde::Serialize) -> Self {
        let name = format!(":cnd_{}", name.trim_start_matches(':'));
        let value = serde_dynamo::to_attribute_value(value).unwrap();
        self.sensitive_values.push((name, value));
        self
    }
}

impl fmt::Debug for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Condition")
            .field("expression", &self.expression)
            .field("names", &self.names)
            .field("values", &self.values)
            .field(
                "sensitive_values",
                &format_args!("<{} values>", self.sensitive_values.len()),
            )
            .finish()
    }
}

/// A compiled projection expression
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub struct Projection {
    /// The parameterized expression
    pub expression: String,

    /// The attribute names used in the expression
    pub names: Vec<(String, String)>,
}

/// A static compiled projection expression
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub struct StaticProjection {
    /// The parameterized expression
    pub expression: &'static str,

    /// The attribute names used in the expression
    pub names: &'static [(&'static str, &'static str)],
}

impl Projection {
    /// Create a new projection expression from a set of attribute names
    pub fn new<'a, I>(attr_names: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let reserved_words = Self::reserved_words();

        let mut seen = FnvHashSet::default();
        let mut expression = String::with_capacity(512);
        let mut names = Vec::new();
        let mut count = 0u32;

        for s in attr_names {
            if !seen.insert(s) {
                continue;
            }

            const LONGEST_RESERVED: usize = 14;
            let reserved = if s.len() <= LONGEST_RESERVED {
                let mut buf = [0u8; LONGEST_RESERVED];
                let len = LONGEST_RESERVED.min(s.len());
                let buf = &mut buf[..len];
                buf.copy_from_slice(&s.as_bytes()[..len]);
                buf.make_ascii_uppercase();
                reserved_words.contains(buf)
            } else {
                false
            };

            let is_invalid = |c: u8| !c.is_ascii_alphanumeric() && c != b'_';
            if reserved || s.bytes().any(is_invalid) {
                let var = format!("#prj_{count:03}");
                count += 1;
                expression.push_str(&var);
                names.push((var, s.into()));
            } else {
                expression.push_str(s);
            }
            expression.push(',');
        }
        expression.truncate(expression.len().saturating_sub(1));

        Self { expression, names }
    }

    #[inline]
    pub(crate) fn leak(self) -> StaticProjection {
        StaticProjection {
            expression: Box::leak(self.expression.into_boxed_str()),
            names: Box::leak(
                self.names
                    .into_iter()
                    .map(|(l, r)| {
                        (
                            &*Box::leak(l.into_boxed_str()),
                            &*Box::leak(r.into_boxed_str()),
                        )
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
        }
    }

    fn reserved_words() -> &'static FnvHashSet<&'static [u8]> {
        #[cfg(not(feature = "once_cell"))]
        static RESERVED_WORDS_SET: std::sync::OnceLock<FnvHashSet<&'static [u8]>> =
            std::sync::OnceLock::new();

        #[cfg(feature = "once_cell")]
        static RESERVED_WORDS_SET: once_cell::sync::OnceCell<FnvHashSet<&'static [u8]>> =
            once_cell::sync::OnceCell::new();

        RESERVED_WORDS_SET.get_or_init(|| {
            Self::RESERVED_WORDS
                .iter()
                .copied()
                .map(|s| s.as_bytes())
                .collect()
        })
    }

    const RESERVED_WORDS: &'static [&'static str] = &[
        "ABORT",
        "ABSOLUTE",
        "ACTION",
        "ADD",
        "AFTER",
        "AGENT",
        "AGGREGATE",
        "ALL",
        "ALLOCATE",
        "ALTER",
        "ANALYZE",
        "AND",
        "ANY",
        "ARCHIVE",
        "ARE",
        "ARRAY",
        "AS",
        "ASC",
        "ASCII",
        "ASENSITIVE",
        "ASSERTION",
        "ASYMMETRIC",
        "AT",
        "ATOMIC",
        "ATTACH",
        "ATTRIBUTE",
        "AUTH",
        "AUTHORIZATION",
        "AUTHORIZE",
        "AUTO",
        "AVG",
        "BACK",
        "BACKUP",
        "BASE",
        "BATCH",
        "BEFORE",
        "BEGIN",
        "BETWEEN",
        "BIGINT",
        "BINARY",
        "BIT",
        "BLOB",
        "BLOCK",
        "BOOLEAN",
        "BOTH",
        "BREADTH",
        "BUCKET",
        "BULK",
        "BY",
        "BYTE",
        "CALL",
        "CALLED",
        "CALLING",
        "CAPACITY",
        "CASCADE",
        "CASCADED",
        "CASE",
        "CAST",
        "CATALOG",
        "CHAR",
        "CHARACTER",
        "CHECK",
        "CLASS",
        "CLOB",
        "CLOSE",
        "CLUSTER",
        "CLUSTERED",
        "CLUSTERING",
        "CLUSTERS",
        "COALESCE",
        "COLLATE",
        "COLLATION",
        "COLLECTION",
        "COLUMN",
        "COLUMNS",
        "COMBINE",
        "COMMENT",
        "COMMIT",
        "COMPACT",
        "COMPILE",
        "COMPRESS",
        "CONDITION",
        "CONFLICT",
        "CONNECT",
        "CONNECTION",
        "CONSISTENCY",
        "CONSISTENT",
        "CONSTRAINT",
        "CONSTRAINTS",
        "CONSTRUCTOR",
        "CONSUMED",
        "CONTINUE",
        "CONVERT",
        "COPY",
        "CORRESPONDING",
        "COUNT",
        "COUNTER",
        "CREATE",
        "CROSS",
        "CUBE",
        "CURRENT",
        "CURSOR",
        "CYCLE",
        "DATA",
        "DATABASE",
        "DATE",
        "DATETIME",
        "DAY",
        "DEALLOCATE",
        "DEC",
        "DECIMAL",
        "DECLARE",
        "DEFAULT",
        "DEFERRABLE",
        "DEFERRED",
        "DEFINE",
        "DEFINED",
        "DEFINITION",
        "DELETE",
        "DELIMITED",
        "DEPTH",
        "DEREF",
        "DESC",
        "DESCRIBE",
        "DESCRIPTOR",
        "DETACH",
        "DETERMINISTIC",
        "DIAGNOSTICS",
        "DIRECTORIES",
        "DISABLE",
        "DISCONNECT",
        "DISTINCT",
        "DISTRIBUTE",
        "DO",
        "DOMAIN",
        "DOUBLE",
        "DROP",
        "DUMP",
        "DURATION",
        "DYNAMIC",
        "EACH",
        "ELEMENT",
        "ELSE",
        "ELSEIF",
        "EMPTY",
        "ENABLE",
        "END",
        "EQUAL",
        "EQUALS",
        "ERROR",
        "ESCAPE",
        "ESCAPED",
        "EVAL",
        "EVALUATE",
        "EXCEEDED",
        "EXCEPT",
        "EXCEPTION",
        "EXCEPTIONS",
        "EXCLUSIVE",
        "EXEC",
        "EXECUTE",
        "EXISTS",
        "EXIT",
        "EXPLAIN",
        "EXPLODE",
        "EXPORT",
        "EXPRESSION",
        "EXTENDED",
        "EXTERNAL",
        "EXTRACT",
        "FAIL",
        "FALSE",
        "FAMILY",
        "FETCH",
        "FIELDS",
        "FILE",
        "FILTER",
        "FILTERING",
        "FINAL",
        "FINISH",
        "FIRST",
        "FIXED",
        "FLATTERN",
        "FLOAT",
        "FOR",
        "FORCE",
        "FOREIGN",
        "FORMAT",
        "FORWARD",
        "FOUND",
        "FREE",
        "FROM",
        "FULL",
        "FUNCTION",
        "FUNCTIONS",
        "GENERAL",
        "GENERATE",
        "GET",
        "GLOB",
        "GLOBAL",
        "GO",
        "GOTO",
        "GRANT",
        "GREATER",
        "GROUP",
        "GROUPING",
        "HANDLER",
        "HASH",
        "HAVE",
        "HAVING",
        "HEAP",
        "HIDDEN",
        "HOLD",
        "HOUR",
        "IDENTIFIED",
        "IDENTITY",
        "IF",
        "IGNORE",
        "IMMEDIATE",
        "IMPORT",
        "IN",
        "INCLUDING",
        "INCLUSIVE",
        "INCREMENT",
        "INCREMENTAL",
        "INDEX",
        "INDEXED",
        "INDEXES",
        "INDICATOR",
        "INFINITE",
        "INITIALLY",
        "INLINE",
        "INNER",
        "INNTER",
        "INOUT",
        "INPUT",
        "INSENSITIVE",
        "INSERT",
        "INSTEAD",
        "INT",
        "INTEGER",
        "INTERSECT",
        "INTERVAL",
        "INTO",
        "INVALIDATE",
        "IS",
        "ISOLATION",
        "ITEM",
        "ITEMS",
        "ITERATE",
        "JOIN",
        "KEY",
        "KEYS",
        "LAG",
        "LANGUAGE",
        "LARGE",
        "LAST",
        "LATERAL",
        "LEAD",
        "LEADING",
        "LEAVE",
        "LEFT",
        "LENGTH",
        "LESS",
        "LEVEL",
        "LIKE",
        "LIMIT",
        "LIMITED",
        "LINES",
        "LIST",
        "LOAD",
        "LOCAL",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "LOCATION",
        "LOCATOR",
        "LOCK",
        "LOCKS",
        "LOG",
        "LOGED",
        "LONG",
        "LOOP",
        "LOWER",
        "MAP",
        "MATCH",
        "MATERIALIZED",
        "MAX",
        "MAXLEN",
        "MEMBER",
        "MERGE",
        "METHOD",
        "METRICS",
        "MIN",
        "MINUS",
        "MINUTE",
        "MISSING",
        "MOD",
        "MODE",
        "MODIFIES",
        "MODIFY",
        "MODULE",
        "MONTH",
        "MULTI",
        "MULTISET",
        "NAME",
        "NAMES",
        "NATIONAL",
        "NATURAL",
        "NCHAR",
        "NCLOB",
        "NEW",
        "NEXT",
        "NO",
        "NONE",
        "NOT",
        "NULL",
        "NULLIF",
        "NUMBER",
        "NUMERIC",
        "OBJECT",
        "OF",
        "OFFLINE",
        "OFFSET",
        "OLD",
        "ON",
        "ONLINE",
        "ONLY",
        "OPAQUE",
        "OPEN",
        "OPERATOR",
        "OPTION",
        "OR",
        "ORDER",
        "ORDINALITY",
        "OTHER",
        "OTHERS",
        "OUT",
        "OUTER",
        "OUTPUT",
        "OVER",
        "OVERLAPS",
        "OVERRIDE",
        "OWNER",
        "PAD",
        "PARALLEL",
        "PARAMETER",
        "PARAMETERS",
        "PARTIAL",
        "PARTITION",
        "PARTITIONED",
        "PARTITIONS",
        "PATH",
        "PERCENT",
        "PERCENTILE",
        "PERMISSION",
        "PERMISSIONS",
        "PIPE",
        "PIPELINED",
        "PLAN",
        "POOL",
        "POSITION",
        "PRECISION",
        "PREPARE",
        "PRESERVE",
        "PRIMARY",
        "PRIOR",
        "PRIVATE",
        "PRIVILEGES",
        "PROCEDURE",
        "PROCESSED",
        "PROJECT",
        "PROJECTION",
        "PROPERTY",
        "PROVISIONING",
        "PUBLIC",
        "PUT",
        "QUERY",
        "QUIT",
        "QUORUM",
        "RAISE",
        "RANDOM",
        "RANGE",
        "RANK",
        "RAW",
        "READ",
        "READS",
        "REAL",
        "REBUILD",
        "RECORD",
        "RECURSIVE",
        "REDUCE",
        "REF",
        "REFERENCE",
        "REFERENCES",
        "REFERENCING",
        "REGEXP",
        "REGION",
        "REINDEX",
        "RELATIVE",
        "RELEASE",
        "REMAINDER",
        "RENAME",
        "REPEAT",
        "REPLACE",
        "REQUEST",
        "RESET",
        "RESIGNAL",
        "RESOURCE",
        "RESPONSE",
        "RESTORE",
        "RESTRICT",
        "RESULT",
        "RETURN",
        "RETURNING",
        "RETURNS",
        "REVERSE",
        "REVOKE",
        "RIGHT",
        "ROLE",
        "ROLES",
        "ROLLBACK",
        "ROLLUP",
        "ROUTINE",
        "ROW",
        "ROWS",
        "RULE",
        "RULES",
        "SAMPLE",
        "SATISFIES",
        "SAVE",
        "SAVEPOINT",
        "SCAN",
        "SCHEMA",
        "SCOPE",
        "SCROLL",
        "SEARCH",
        "SECOND",
        "SECTION",
        "SEGMENT",
        "SEGMENTS",
        "SELECT",
        "SELF",
        "SEMI",
        "SENSITIVE",
        "SEPARATE",
        "SEQUENCE",
        "SERIALIZABLE",
        "SESSION",
        "SET",
        "SETS",
        "SHARD",
        "SHARE",
        "SHARED",
        "SHORT",
        "SHOW",
        "SIGNAL",
        "SIMILAR",
        "SIZE",
        "SKEWED",
        "SMALLINT",
        "SNAPSHOT",
        "SOME",
        "SOURCE",
        "SPACE",
        "SPACES",
        "SPARSE",
        "SPECIFIC",
        "SPECIFICTYPE",
        "SPLIT",
        "SQL",
        "SQLCODE",
        "SQLERROR",
        "SQLEXCEPTION",
        "SQLSTATE",
        "SQLWARNING",
        "START",
        "STATE",
        "STATIC",
        "STATUS",
        "STORAGE",
        "STORE",
        "STORED",
        "STREAM",
        "STRING",
        "STRUCT",
        "STYLE",
        "SUB",
        "SUBMULTISET",
        "SUBPARTITION",
        "SUBSTRING",
        "SUBTYPE",
        "SUM",
        "SUPER",
        "SYMMETRIC",
        "SYNONYM",
        "SYSTEM",
        "TABLE",
        "TABLESAMPLE",
        "TEMP",
        "TEMPORARY",
        "TERMINATED",
        "TEXT",
        "THAN",
        "THEN",
        "THROUGHPUT",
        "TIME",
        "TIMESTAMP",
        "TIMEZONE",
        "TINYINT",
        "TO",
        "TOKEN",
        "TOTAL",
        "TOUCH",
        "TRAILING",
        "TRANSACTION",
        "TRANSFORM",
        "TRANSLATE",
        "TRANSLATION",
        "TREAT",
        "TRIGGER",
        "TRIM",
        "TRUE",
        "TRUNCATE",
        "TTL",
        "TUPLE",
        "TYPE",
        "UNDER",
        "UNDO",
        "UNION",
        "UNIQUE",
        "UNIT",
        "UNKNOWN",
        "UNLOGGED",
        "UNNEST",
        "UNPROCESSED",
        "UNSIGNED",
        "UNTIL",
        "UPDATE",
        "UPPER",
        "URL",
        "USAGE",
        "USE",
        "USER",
        "USERS",
        "USING",
        "UUID",
        "VACUUM",
        "VALUE",
        "VALUED",
        "VALUES",
        "VARCHAR",
        "VARIABLE",
        "VARIANCE",
        "VARINT",
        "VARYING",
        "VIEW",
        "VIEWS",
        "VIRTUAL",
        "VOID",
        "WAIT",
        "WHEN",
        "WHENEVER",
        "WHERE",
        "WHILE",
        "WINDOW",
        "WITH",
        "WITHIN",
        "WITHOUT",
        "WORK",
        "WRAPPED",
        "WRITE",
        "YEAR",
        "ZONE",
    ];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_expected_substitutions_for_projection_expression() {
        const TEST_SET: &[&str] = &[
            "hello",
            "user_id",
            "window",
            "news😛",
            "windowed",
            "face",
            "unprocessed.stuff",
            "void",
            "reader",
        ];

        let proj = Projection::new(TEST_SET.iter().copied());

        assert_eq!(
            proj.expression,
            "hello,user_id,#prj_000,#prj_001,windowed,face,#prj_002,#prj_003,reader"
        );
        assert_eq!(
            proj.names,
            vec![
                ("#prj_000".to_owned(), "window".to_owned()),
                ("#prj_001".to_owned(), "news😛".to_owned()),
                ("#prj_002".to_owned(), "unprocessed.stuff".to_owned()),
                ("#prj_003".to_owned(), "void".to_owned())
            ]
        );
    }

    #[test]
    fn projection_expression_filters_out_duplicates() {
        const TEST_SET: &[&str] = &["alpha", "void", "beta", "alpha", "void", "green"];

        let proj = Projection::new(TEST_SET.iter().copied());

        assert_eq!(proj.expression, "alpha,#prj_000,beta,green");
        assert_eq!(proj.names, vec![("#prj_000".to_owned(), "void".to_owned())]);
    }
}
