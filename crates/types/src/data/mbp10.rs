//! MBP-10 record type (Databento-style aggregated book updates)
//! "Base data type" for MDP-10/MBP-10 translated into our engine types.
use crate::securities::symbols::Instrument;
use crate::wire::Bytes;
use chrono::TimeDelta;
pub use chrono::{DateTime, Utc};
use rkyv::{AlignedVec, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;
use strum_macros::Display;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Archive, RkyvDeserialize, RkyvSerialize,
)]
#[archive(check_bytes)]
pub enum BookSide {
    Ask,  // 'A'
    Bid,  // 'B'
    None, // 'N' or unknown
}

impl From<u8> for BookSide {
    fn from(b: u8) -> Self {
        match b {
            b'A' => BookSide::Ask,
            b'B' => BookSide::Bid,
            b'N' => BookSide::None,
            _ => BookSide::None, // default fallback
        }
    }
}

impl From<BookSide> for u8 {
    fn from(s: BookSide) -> u8 {
        match s {
            BookSide::Ask => b'A',
            BookSide::Bid => b'B',
            BookSide::None => b'N',
        }
    }
}
/// Order/event action (unknown bytes map to `None`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub enum Action {
    Add,    // 'A'
    Modify, // 'M'
    Cancel, // 'C'
    Clear,  // 'R'
    Trade,  // 'T'
    Fill,   // 'F'
    None,   // 'N' or unknown
}

impl From<u8> for Action {
    fn from(b: u8) -> Self {
        match b {
            b'A' => Action::Add,
            b'M' => Action::Modify,
            b'C' => Action::Cancel,
            b'R' => Action::Clear,
            b'T' => Action::Trade,
            b'F' => Action::Fill,
            b'N' => Action::None,
            _ => Action::None,
        }
    }
}

impl From<Action> for u8 {
    fn from(a: Action) -> u8 {
        match a {
            Action::Add => b'A',
            Action::Modify => b'M',
            Action::Cancel => b'C',
            Action::Clear => b'R',
            Action::Trade => b'T',
            Action::Fill => b'F',
            Action::None => b'N',
        }
    }
}

/// Flags bitfield (serde/rkyv-friendly newtype).
#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct Flags(pub u8);

impl Flags {
    pub const F_LAST: u8 = 1 << 7; // 128
    pub const F_TOB: u8 = 1 << 6; // 64
    pub const F_SNAPSHOT: u8 = 1 << 5; // 32
    pub const F_MBP: u8 = 1 << 4; // 16
    pub const F_BAD_TS_RECV: u8 = 1 << 3; // 8
    pub const F_MAYBE_BAD_BOOK: u8 = 1 << 2; // 4
    pub const F_PUBLISHER_SPECIFIC: u8 = 1 << 1; // 2

    #[inline]
    pub fn contains(self, mask: u8) -> bool {
        self.0 & mask != 0
    }
    #[inline]
    pub fn is_last(self) -> bool {
        self.contains(Self::F_LAST)
    }
    #[inline]
    pub fn is_tob(self) -> bool {
        self.contains(Self::F_TOB)
    }
    #[inline]
    pub fn is_snapshot(self) -> bool {
        self.contains(Self::F_SNAPSHOT)
    }
    #[inline]
    pub fn is_mbp(self) -> bool {
        self.contains(Self::F_MBP)
    }
    #[inline]
    pub fn bad_ts_recv(self) -> bool {
        self.contains(Self::F_BAD_TS_RECV)
    }
    #[inline]
    pub fn maybe_bad_book(self) -> bool {
        self.contains(Self::F_MAYBE_BAD_BOOK)
    }
}

impl From<u8> for Flags {
    fn from(v: u8) -> Self {
        Flags(v)
    }
}
impl From<Flags> for u8 {
    fn from(f: Flags) -> u8 {
        f.0
    }
}

/// Optional aggregated book levels. When present, vectors align by index as level depth.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct BookLevels {
    // Price vectors in integer nanos (scale 1e-9) for archive-friendly representation.
    pub bid_px: Vec<Decimal>,
    pub ask_px: Vec<Decimal>,
    pub bid_sz: Vec<Decimal>,
    pub ask_sz: Vec<Decimal>,
    pub bid_ct: Vec<Decimal>,
    pub ask_ct: Vec<Decimal>,
}

impl BookLevels {
    pub fn empty() -> Self {
        Self {
            bid_px: vec![],
            ask_px: vec![],
            bid_sz: vec![],
            ask_sz: vec![],
            bid_ct: vec![],
            ask_ct: vec![],
        }
    }
}

/// MBP-10 record mapped to engine-friendly types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct Mbp10 {
    pub instrument: Instrument,
    /// Capture-server receive time (UTC).
    pub ts_recv: DateTime<Utc>,
    /// Matching engine receive time (UTC).
    pub ts_event: DateTime<Utc>,

    /// Record type sentinel (always 10 for MBP-10).
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

    /// Optional aggregated book snapshot/levels.
    pub book: Option<BookLevels>,
}

impl Bytes<Self> for Mbp10 {
    fn from_bytes(archived: &[u8]) -> anyhow::Result<Mbp10> {
        match rkyv::from_bytes::<Mbp10>(&archived) {
            Ok(response) => Ok(response),
            Err(e) => Err(anyhow::Error::msg(e.to_string())),
        }
    }

    fn to_aligned_bytes(&self) -> AlignedVec {
        // Serialize directly into an AlignedVec for maximum compatibility with rkyv
        rkyv::to_bytes::<_, 1024>(self).expect("rkyv::to_bytes failed")
    }

    fn to_bytes(&self) -> Vec<u8> {
        let vec = rkyv::to_bytes::<_, 1024>(self).unwrap();
        vec.into()
    }
}

/// Build an `Mbp10` from wire-precision primitives.
///
/// * `*_ns` fields are UNIX epoch nanoseconds.
/// * `price_nanos` and price vectors are integer nanos (1e-9) and converted to `Decimal` scale 9.
pub fn make_mbp10(
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
    // Optional aggregated levels; pass `None` if not present.
    levels: Option<(
        Vec<Decimal>, // bid_px nanos
        Vec<Decimal>, // ask_px nanos
        Vec<Decimal>, // bid_sz
        Vec<Decimal>, // ask_sz
        Vec<Decimal>, // bid_ct
        Vec<Decimal>, // ask_ct
    )>,
) -> Option<Mbp10> {
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

    let book = levels.map(|(bp, ap, bsz, asz, bct, act)| BookLevels {
        bid_px: bp,
        ask_px: ap,
        bid_sz: bsz,
        ask_sz: asz,
        bid_ct: bct,
        ask_ct: act,
    });

    Some(Mbp10 {
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
        book,
    })
}

/// Convenience: compute the matching-engine send time as `ts_recv - ts_in_delta`.
#[inline]
pub fn calc_ts_in(ts_recv: DateTime<Utc>, ts_in_delta: i32) -> Option<DateTime<Utc>> {
    // ts_in_delta is "nanoseconds before ts_recv"; may be negative in theory.
    let delta = TimeDelta::nanoseconds(ts_in_delta as i64);
    ts_recv.checked_sub_signed(delta)
}
