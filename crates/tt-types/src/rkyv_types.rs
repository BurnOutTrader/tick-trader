use chrono::TimeZone;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

// Remote derive wrappers and helpers for external types that don't natively implement rkyv

// ===== chrono::DateTime<Utc> =====
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[rkyv(remote = chrono::DateTime<chrono::Utc>)]
pub struct DateTimeUtcDef {
    // Store as UNIX epoch nanoseconds for compactness and precision
    #[rkyv(getter = crate::rkyv_types::dt_to_ns)]
    pub ts_ns: i64,
}

pub fn dt_to_ns(dt: &chrono::DateTime<chrono::Utc>) -> i64 {
    // Prefer nanos if available (always on Unix time range), fallback to micros
    dt.timestamp_nanos_opt()
        .unwrap_or_else(|| dt.timestamp_micros() * 1_000)
}

impl From<DateTimeUtcDef> for chrono::DateTime<chrono::Utc> {
    fn from(value: DateTimeUtcDef) -> Self {
        let secs = value.ts_ns.div_euclid(1_000_000_000);
        let nsub = value.ts_ns.rem_euclid(1_000_000_000) as u32;
        chrono::Utc.timestamp_opt(secs, nsub).single().expect("invalid ts")
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

pub fn dec_mantissa(d: &rust_decimal::Decimal) -> i128 { d.mantissa() }
pub fn dec_scale(d: &rust_decimal::Decimal) -> u32 { d.scale() as u32 }

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
pub use DecimalDef as RkyvDecimal;
pub use DateTimeUtcDef as RkyvDateTimeUtc;
