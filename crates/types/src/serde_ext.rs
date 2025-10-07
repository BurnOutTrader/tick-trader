use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::ser::SerializeSeq;
use serde::de::{self, SeqAccess, Visitor};
use std::fmt;
use rust_decimal::Decimal;

/// Serde helpers for bincode-friendly Decimal encoding.
///
/// Binary formats like bincode do not support `deserialize_any`, which the default
/// `rust_decimal` implementation can use to be flexible. These helpers encode
/// Decimal explicitly as a (mantissa i128, scale u32) tuple for binary formats,
/// while keeping a human-readable string representation for text formats (JSON, etc.).
pub mod decimal {
    use super::*;

    pub fn serialize<S>(value: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            return serializer.serialize_str(&value.to_string());
        }
        let mantissa: i128 = value.mantissa();
        let scale: u32 = value.scale();
        (mantissa, scale).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            return Decimal::from_str_exact(&s).map_err(de::Error::custom);
        }
        let (mantissa, scale): (i128, u32) = <(i128, u32)>::deserialize(deserializer)?;
        Ok(Decimal::from_i128_with_scale(mantissa, scale))
    }
}

/// Serde helpers for Vec<Decimal> using the same strategy as `decimal`.
pub mod decimal_vec {
    use super::*;

    pub fn serialize<S>(values: &Vec<Decimal>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            let mut seq = serializer.serialize_seq(Some(values.len()))?;
            for d in values.iter() {
                seq.serialize_element(&d.to_string())?;
            }
            return seq.end();
        }
        // Binary: encode as Vec<(i128, u32)>
        let mut seq = serializer.serialize_seq(Some(values.len()))?;
        for d in values.iter() {
            let pair = (d.mantissa(), d.scale());
            seq.serialize_element(&pair)?;
        }
        seq.end()
    }

    struct DecimalVecVisitorHuman;
    impl<'de> Visitor<'de> for DecimalVecVisitorHuman {
        type Value = Vec<Decimal>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "a sequence of decimal strings")
        }
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(s) = seq.next_element::<String>()? {
                let d = Decimal::from_str_exact(&s).map_err(de::Error::custom)?;
                out.push(d);
            }
            Ok(out)
        }
    }

    struct DecimalVecVisitorBinary;
    impl<'de> Visitor<'de> for DecimalVecVisitorBinary {
        type Value = Vec<Decimal>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "a sequence of (i128, u32) tuples")
        }
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some((mantissa, scale)) = seq.next_element::<(i128, u32)>()? {
                let d = Decimal::from_i128_with_scale(mantissa, scale);
                out.push(d);
            }
            Ok(out)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Decimal>, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            deserializer.deserialize_seq(DecimalVecVisitorHuman)
        } else {
            deserializer.deserialize_seq(DecimalVecVisitorBinary)
        }
    }
}

pub mod datetime {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde::de::{self};

    pub fn serialize<S>(value: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            return serializer.serialize_str(&value.to_rfc3339());
        }
        let secs = value.timestamp();
        let nanos = value.timestamp_subsec_nanos();
        (secs, nanos).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let dt = chrono::DateTime::parse_from_rfc3339(&s)
                .map_err(de::Error::custom)?
                .with_timezone(&Utc);
            return Ok(dt);
        }
        let (secs, nanos): (i64, u32) = <(i64, u32)>::deserialize(deserializer)?;
        chrono::DateTime::from_timestamp(secs, nanos)
            .ok_or_else(|| de::Error::custom("invalid (secs,nanos) for DateTime<Utc>"))
    }
}

// Binary-friendly serde helpers for NaiveDate and Option<NaiveDate>.
pub mod naivedate {
    use chrono::NaiveDate;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde::de::{self};

    pub fn serialize<S>(value: &NaiveDate, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            return serializer.serialize_str(&value.to_string());
        }
        let y = value.year();
        let m = value.month();
        let d = value.day();
        (y, m as u32, d as u32).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            return NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(de::Error::custom);
        }
        let (y, m, d): (i32, u32, u32) = <(i32, u32, u32)>::deserialize(deserializer)?;
        NaiveDate::from_ymd_opt(y, m, d).ok_or_else(|| de::Error::custom("invalid NaiveDate components"))
    }
}
