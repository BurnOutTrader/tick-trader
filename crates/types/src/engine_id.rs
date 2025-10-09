use crate::wire::ENGINE_TAG_PREFIX;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagSource {
    Extracted, // found in user tag
    Generated, // missing/stripped, generated new
}

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

    /// Extract the engine tag and any trailing text.
    /// Returns `(EngineTag, rest_after_id)`
    pub fn extract_engine_uuid(s: &str) -> Option<(EngineUuid, Option<String>)> {
        let pos = s.find(ENGINE_TAG_PREFIX)?;
        let start = pos + ENGINE_TAG_PREFIX.len();
        let after = &s[start..];

        // Split at first delimiter
        let mut it = after.splitn(2, &[' ', ';', '+'][..]);
        let id_str = it.next().unwrap();
        let rest = it.next().map(|t| t.to_string());

        let tag = EngineUuid::from_hex32(id_str)?;
        Some((tag, rest))
    }

    /// Try to extract EngineTag and return the sanitized user tag (with engine tag removed).
    /// If missing, generate a new EngineTag and return the original tag unchanged.
    pub fn derive_engine_tag(user_tag: Option<&str>) -> (EngineUuid, Option<String>, TagSource) {
        match user_tag {
            Some(t) => {
                if let Some((eng, sanitized)) = EngineUuid::extract_and_remove_engine_tag(t) {
                    (eng, sanitized, TagSource::Extracted)
                } else {
                    (EngineUuid::new(), Some(t.to_string()), TagSource::Generated)
                }
            }
            None => (EngineUuid::new(), None, TagSource::Generated),
        }
    }

    /// Extracts the engine tag from `s` and returns (EngineTag, sanitized_tag_without_engine_tag).
    /// If prefix not found or malformed, returns None.
    pub fn extract_and_remove_engine_tag(s: &str) -> Option<(EngineUuid, Option<String>)> {
        let pos = s.find(ENGINE_TAG_PREFIX)?;
        let id_start = pos + ENGINE_TAG_PREFIX.len();

        // Find end of the ID by scanning until a delimiter or end.
        let bytes = s.as_bytes();
        let mut id_end = id_start;
        while id_end < s.len() {
            let c = bytes[id_end] as char;
            if c == ' ' || c == ';' || c == '+' {
                break;
            }
            id_end += 1;
        }

        // Parse the hex32 ID substring.
        let id_str = &s[id_start..id_end];
        let eng = EngineUuid::from_hex32(id_str)?;

        // Build sanitized string: everything before prefix + everything after ID (skipping one optional delimiter)
        let mut out = String::new();
        out.push_str(&s[..pos]); // before prefix

        // Skip exactly one delimiter if present after the ID to avoid leftover "+", ";", or space
        let mut after = id_end;
        if after < s.len() {
            let c = bytes[after] as char;
            if c == ' ' || c == ';' || c == '+' {
                after += 1;
            }
        }
        out.push_str(&s[after..]);

        // Normalize: trim redundant delimiters at boundaries
        let sanitized = out
            .trim_matches(|c: char| c == '+' || c == ';' || c == ' ')
            .to_string();
        let sanitized = if sanitized.is_empty() {
            None
        } else {
            Some(sanitized)
        };

        Some((eng, sanitized))
    }

    /// Append engine tag to an optional existing (sanitized) tag.
    pub fn append_engine_tag(existing: Option<String>, eng: EngineUuid) -> String {
        let id_str = eng.to_hex32();
        match existing {
            Some(mut t) => {
                if !t.is_empty() && !t.ends_with('+') && !t.ends_with(';') && !t.ends_with(' ') {
                    t.push('+'); // consistent delimiter
                }
                t.push_str(ENGINE_TAG_PREFIX);
                t.push_str(&id_str);
                t
            }
            None => format!("{}{}", ENGINE_TAG_PREFIX, id_str),
        }
    }
}

impl fmt::Display for EngineUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}
