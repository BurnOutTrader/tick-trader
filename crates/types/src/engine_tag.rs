use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Compact, rkyv-friendly identifier for engine-level tagging.
/// Stored internally as a `u128` (16 bytes, zero-copy, no parsing).
#[derive(
    Clone,
    Debug,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Serialize,
    Deserialize,
)]
#[archive(check_bytes)]
pub struct EngineUuid(pub u128);

impl EngineUuid {
    /// Generate a new random EngineTag (UUID v4)
    pub fn new() -> Self {
        Self(Uuid::new_v4().as_u128())
    }

    /// Convert to canonical UUID
    pub fn to_uuid(self) -> Uuid {
        Uuid::from_u128(self.0)
    }

    /// Convert from UUID
    pub fn from_uuid(u: Uuid) -> Self {
        Self(u.as_u128())
    }

    /// Encode as 32-character lowercase hex (broker-safe alphanumeric)
    pub fn to_hex32(self) -> String {
        format!("{:032x}", self.0)
    }

    /// Decode from 32-character hex string
    pub fn from_hex32(s: &str) -> Option<Self> {
        if s.len() == 32 {
            u128::from_str_radix(s, 16).ok().map(Self)
        } else {
            None
        }
    }
}

impl fmt::Display for EngineUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}
