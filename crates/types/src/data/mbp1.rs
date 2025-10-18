//! MBP-1 record type (Databento-style top-of-book updates mapped to engine types)
//! Similar to `mbp10`, but constrained to BBO (best bid/offer) fields.

use crate::data::mbp10::{Action, BookSide, Flags};
use crate::securities::symbols::Instrument;
use crate::wire::Bytes;
use chrono::TimeDelta;
pub use chrono::{DateTime, Utc};
use rkyv::{AlignedVec, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;

/// MBP-1 (market-by-price, top of book only) mapped to engine-friendly types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct Mbp1 {
    pub instrument: Instrument,
    /// Capture-server receive time (UTC).
    pub ts_recv: DateTime<Utc>,
    /// Matching engine receive time (UTC).
    pub ts_event: DateTime<Utc>,

    /// Record type sentinel (always 1 for MBP-1 in Databento schema, but we carry through provided value).
    pub rtype: u8,
    pub publisher_id: u16,
    pub instrument_id: u32,

    pub action: Action,
    pub side: BookSide,

    /// Book level where the update occurred.
    pub depth: u8,

    /// Order price (wire is i64 nanos). Stored as Decimal (scale 9).
    pub price: Decimal,

    /// Order quantity (Decimal, e.g., contracts or shares with fractional support).
    pub size: Decimal,

    pub flags: Flags,

    /// Matching engine sending ts = ts_recv - ts_in_delta (nanos).
    pub ts_in_delta: i32,

    /// Venue sequence number.
    pub sequence: u32,

    // Top-of-book snapshot at time of this update
    pub bid_px_00: Decimal,
    pub ask_px_00: Decimal,
    pub bid_sz_00: Decimal,
    pub ask_sz_00: Decimal,
    pub bid_ct_00: u32,
    pub ask_ct_00: u32,
}

impl Bytes<Self> for Mbp1 {
    fn from_bytes(archived: &[u8]) -> anyhow::Result<Mbp1> {
        match rkyv::from_bytes::<Mbp1>(archived) {
            Ok(response) => Ok(response),
            Err(e) => Err(anyhow::Error::msg(e.to_string())),
        }
    }

    fn to_aligned_bytes(&self) -> AlignedVec {
        // Serialize directly into an AlignedVec for maximum compatibility with rkyv
        rkyv::to_bytes::<_, 1024>(self).expect("rkyv::to_bytes failed")
    }
}

/// Build an `Mbp1` from wire-precision primitives.
///
/// - `*_ns` fields are UNIX epoch nanoseconds.
/// - `*_px_nanos` and `price_nanos` are integer nanos (1e-9) converted to `Decimal` scale 9.
#[allow(clippy::too_many_arguments)]
pub fn make_mbp1(
    instrument: Instrument,
    ts_recv_ns: u64,
    ts_event_ns: u64,
    rtype: u8,
    publisher_id: u16,
    instrument_id: u32,
    action_b: u8,
    side_b: u8,
    depth: u8,
    price_nanos: i64,
    size: Decimal,
    flags_b: u8,
    ts_in_delta: i32,
    sequence: u32,
    bid_px_nanos: i64,
    ask_px_nanos: i64,
    bid_sz: Decimal,
    ask_sz: Decimal,
    bid_ct: u32,
    ask_ct: u32,
) -> Option<Mbp1> {
    // Convert epoch nanos to DateTime<Utc> safely.
    fn dt_from_ns(ns: u64) -> Option<DateTime<Utc>> {
        let secs = (ns / 1_000_000_000) as i64;
        let sub = (ns % 1_000_000_000) as u32;
        DateTime::from_timestamp(secs, sub)
    }

    let ts_recv = dt_from_ns(ts_recv_ns)?;
    let ts_event = dt_from_ns(ts_event_ns)?;

    let action = Action::from(action_b);
    let side = BookSide::from(side_b);
    let flags = Flags::from(flags_b);

    let price = Decimal::from_i128_with_scale(price_nanos as i128, 9);
    let bid_px_00 = Decimal::from_i128_with_scale(bid_px_nanos as i128, 9);
    let ask_px_00 = Decimal::from_i128_with_scale(ask_px_nanos as i128, 9);

    Some(Mbp1 {
        instrument,
        ts_recv,
        ts_event,
        rtype,
        publisher_id,
        instrument_id,
        action,
        side,
        depth,
        price,
        size,
        flags,
        ts_in_delta,
        sequence,
        bid_px_00,
        ask_px_00,
        bid_sz_00: bid_sz,
        ask_sz_00: ask_sz,
        bid_ct_00: bid_ct,
        ask_ct_00: ask_ct,
    })
}

/// Convenience: compute the matching-engine send time as `ts_recv - ts_in_delta`.
#[inline]
pub fn calc_ts_in(ts_recv: DateTime<Utc>, ts_in_delta: i32) -> Option<DateTime<Utc>> {
    // ts_in_delta is "nanoseconds before ts_recv"; may be negative in theory.
    let delta = TimeDelta::nanoseconds(ts_in_delta as i64);
    ts_recv.checked_sub_signed(delta)
}
