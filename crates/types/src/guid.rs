use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// 16-byte globally unique identifier used for internal IDs.
/// Stored as raw bytes for compact rkyv/serde representation.
#[derive(
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
#[repr(transparent)]
pub struct Guid(pub [u8; 16]);

impl Guid {
    #[inline]
    pub fn new_v4() -> Self {
        let id = uuid::Uuid::new_v4();
        Self(*id.as_bytes())
    }
}

impl Default for Guid {
    fn default() -> Self { Self::new_v4() }
}

impl From<uuid::Uuid> for Guid {
    fn from(v: uuid::Uuid) -> Self { Self(*v.as_bytes()) }
}

impl From<Guid> for uuid::Uuid {
    fn from(g: Guid) -> Self { uuid::Uuid::from_bytes(g.0) }
}

impl fmt::Display for Guid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let u: uuid::Uuid = (*self).into();
        write!(f, "{}", u)
    }
}

impl FromStr for Guid {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let u = uuid::Uuid::parse_str(s)?;
        Ok(Guid(*u.as_bytes()))
    }
}
