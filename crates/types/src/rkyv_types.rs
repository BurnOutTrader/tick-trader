/*use chrono::{TimeZone, Datelike};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;
use rkyv::with::{ArchiveWith, SerializeWith, DeserializeWith};
use rkyv::{Archive as _, Serialize as _, Deserialize as _};

// Remote derive wrappers and helpers for external types that don't natively implement rkyv

// ===== chrono::DateTime<Utc> =====
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Clone, Copy, PartialEq, Eq, Hash)]

#[rkyv(remote = chrono::DateTime<chrono::Utc>)]
pub struct DateTimeUtcDef {
    // Store as (secs, nanos) to avoid overflow on extreme ranges
    #[rkyv(getter = crate::rkyv_types::dt_secs)]
    pub secs: i64,
    #[rkyv(getter = crate::rkyv_types::dt_nanos)]
    pub nanos: u32,
}

pub fn dt_secs(dt: &chrono::DateTime<chrono::Utc>) -> i64 {
    dt.timestamp()
}

pub fn dt_nanos(dt: &chrono::DateTime<chrono::Utc>) -> u32 {
    dt.timestamp_subsec_nanos()
}

impl From<DateTimeUtcDef> for chrono::DateTime<chrono::Utc> {
    fn from(value: DateTimeUtcDef) -> Self {
        chrono::Utc
            .timestamp_opt(value.secs, value.nanos)
            .single()
            .expect("invalid ts")
    }
}

// ===== rust_decimal::Decimal =====
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Clone, Copy, PartialEq, Eq, Hash)]

#[rkyv(remote = rust_decimal::Decimal)]
pub struct DecimalDef {
    #[rkyv(getter = crate::rkyv_types::dec_mantissa)]
    pub mantissa: i128,
    #[rkyv(getter = crate::rkyv_types::dec_scale)]
    pub scale: u32,
}

pub fn dec_mantissa(d: &rust_decimal::Decimal) -> i128 {
    d.mantissa()
}
pub fn dec_scale(d: &rust_decimal::Decimal) -> u32 {
    d.scale() as u32
}

impl From<DecimalDef> for rust_decimal::Decimal {
    fn from(value: DecimalDef) -> Self {
        rust_decimal::Decimal::from_i128_with_scale(value.mantissa, value.scale)
    }
}

// ===== chrono::NaiveDate =====
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Clone, Copy, PartialEq, Eq, Hash)]

#[rkyv(remote = chrono::NaiveDate)]
pub struct NaiveDateDef {
    #[rkyv(getter = crate::rkyv_types::date_year)]
    pub year: i32,
    #[rkyv(getter = crate::rkyv_types::date_month)]
    pub month: u32,
    #[rkyv(getter = crate::rkyv_types::date_day)]
    pub day: u32,
}

pub fn date_year(d: &chrono::NaiveDate) -> i32 { d.year() }
pub fn date_month(d: &chrono::NaiveDate) -> u32 { d.month() }
pub fn date_day(d: &chrono::NaiveDate) -> u32 { d.day() }

impl From<NaiveDateDef> for chrono::NaiveDate {
    fn from(value: NaiveDateDef) -> Self {
        chrono::NaiveDate::from_ymd_opt(value.year, value.month, value.day)
            .expect("invalid date")
    }
}

// For convenience re-export common names if needed by callers
pub use DateTimeUtcDef as RkyvDateTimeUtc;
pub use DecimalDef as RkyvDecimal;
pub use NaiveDateDef as RkyvNaiveDate;*/
