use chrono::TimeZone;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

// Remote derive wrappers and helpers for external types that don't natively implement rkyv

// ===== chrono::DateTime<Utc> =====
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

// Note on containers (Vec/Option/etc):
// These adapters implement ArchiveWith/SerializeWith/DeserializeWith for the bare types
// (Decimal and DateTime<Utc>) only. In rkyv 0.8, you cannot apply the adapter directly to a
// container like Vec<Decimal> or Option<DateTime<Utc>>. If you need container support, adapt
// the element type (e.g., define a new wrapper type for the element) or create a container-
// specific adapter. The types in this crate use the adapters on direct fields.

// For convenience re-export common names if needed by callers
pub use DateTimeUtcDef as RkyvDateTimeUtc;
pub use DecimalDef as RkyvDecimal;
