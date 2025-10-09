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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::ENGINE_TAG_PREFIX;
    use uuid::Uuid;

    fn tag_str(eng: EngineUuid) -> String {
        format!("{}{}", ENGINE_TAG_PREFIX, eng.to_hex32())
    }

    #[test]
    fn new_and_uuid_roundtrip() {
        let e = EngineUuid::new();
        let u = e.to_uuid();
        assert_eq!(EngineUuid::from_uuid(u), e);
    }

    #[test]
    fn hex32_encoding_decoding_lowercase_and_uppercase() {
        let e = EngineUuid::new();
        let hex = e.to_hex32();
        assert_eq!(hex.len(), 32);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
        // lowercase decode
        let d1 = EngineUuid::from_hex32(&hex).expect("decode lowercase");
        assert_eq!(d1, e);
        // uppercase decode
        let upper = hex.to_uppercase();
        let d2 = EngineUuid::from_hex32(&upper).expect("decode uppercase");
        assert_eq!(d2, e);
    }

    #[test]
    fn hex32_decoding_invalid_inputs() {
        assert!(EngineUuid::from_hex32("abc").is_none(), "too short");
        assert!(
            EngineUuid::from_hex32(&"0".repeat(31)).is_none(),
            "31 chars"
        );
        assert!(
            EngineUuid::from_hex32(&"0".repeat(33)).is_none(),
            "33 chars"
        );
        let mut bad = "0".repeat(31) + "Z"; // non-hex
        assert!(EngineUuid::from_hex32(&bad).is_none(), "non-hex char");
        bad = "x".repeat(32);
        assert!(EngineUuid::from_hex32(&bad).is_none(), "all non-hex");
    }

    #[test]
    fn extract_engine_uuid_no_rest() {
        let e = EngineUuid::new();
        let s = tag_str(e);
        let (ex, rest) = EngineUuid::extract_engine_uuid(&s).expect("extract");
        assert_eq!(ex, e);
        assert!(rest.is_none());
    }

    #[test]
    fn extract_engine_uuid_with_rest_using_delimiters() {
        let e = EngineUuid::new();

        for delim in [' ', ';', '+'] {
            let s = format!("{}{}{}rest", ENGINE_TAG_PREFIX, e.to_hex32(), delim);
            let (ex, rest) = EngineUuid::extract_engine_uuid(&s).expect("extract");
            assert_eq!(ex, e);
            assert_eq!(rest.as_deref(), Some("rest"));
        }
    }

    #[test]
    fn extract_engine_uuid_missing_prefix() {
        let e = EngineUuid::new();
        let s = e.to_hex32(); // no prefix
        assert!(EngineUuid::extract_engine_uuid(&s).is_none());
    }

    #[test]
    fn extract_engine_uuid_malformed_hex_in_stream() {
        let e = EngineUuid::new();
        // valid id followed by extra letters without delimiter makes overall id_str invalid
        let s = format!("{}{}XYZ", ENGINE_TAG_PREFIX, e.to_hex32());
        assert!(EngineUuid::extract_engine_uuid(&s).is_none());
    }

    #[test]
    fn extract_and_remove_engine_tag_only_tag_yields_none_sanitized() {
        let e = EngineUuid::new();
        let s = tag_str(e);
        let (ex, sanitized) =
            EngineUuid::extract_and_remove_engine_tag(&s).expect("extract+remove");
        assert_eq!(ex, e);
        assert!(sanitized.is_none());
    }

    #[test]
    fn extract_and_remove_engine_tag_beginning_with_space_delimiter() {
        let e = EngineUuid::new();
        let s = format!("{}{} rest", ENGINE_TAG_PREFIX, e.to_hex32());
        let (ex, sanitized) =
            EngineUuid::extract_and_remove_engine_tag(&s).expect("extract+remove");
        assert_eq!(ex, e);
        assert_eq!(sanitized.as_deref(), Some("rest"));
    }

    #[test]
    fn extract_and_remove_engine_tag_middle_with_plus_delimiter() {
        let e = EngineUuid::new();
        let s = format!("pre+{}{}+post", ENGINE_TAG_PREFIX, e.to_hex32());
        let (ex, sanitized) =
            EngineUuid::extract_and_remove_engine_tag(&s).expect("extract+remove");
        assert_eq!(ex, e);
        assert_eq!(sanitized.as_deref(), Some("pre+post"));
    }

    #[test]
    fn extract_and_remove_engine_tag_middle_with_semicolon_delimiter() {
        let e = EngineUuid::new();
        let s = format!("left;{}{};right", ENGINE_TAG_PREFIX, e.to_hex32());
        let (ex, sanitized) =
            EngineUuid::extract_and_remove_engine_tag(&s).expect("extract+remove");
        assert_eq!(ex, e);
        assert_eq!(sanitized.as_deref(), Some("left;right"));
    }

    #[test]
    fn extract_and_remove_engine_tag_trims_single_delimiter_after_id() {
        let e = EngineUuid::new();
        // Two delimiters after id -> function skips exactly one, leaving one
        let s = format!("A+{}{}++B", ENGINE_TAG_PREFIX, e.to_hex32());
        let (_, sanitized) = EngineUuid::extract_and_remove_engine_tag(&s).expect("extract+remove");
        assert_eq!(sanitized.as_deref(), Some("A++B"));
    }

    #[test]
    fn extract_and_remove_engine_tag_malformed_returns_none() {
        let s = format!("{}{}", ENGINE_TAG_PREFIX, "0".repeat(31)); // bad length
        assert!(EngineUuid::extract_and_remove_engine_tag(&s).is_none());
    }

    #[test]
    fn derive_engine_tag_when_present_is_extracted_and_sanitized() {
        let e = EngineUuid::new();
        let s = format!("alpha+{}{};beta", ENGINE_TAG_PREFIX, e.to_hex32());
        let (ex, sanitized, src) = EngineUuid::derive_engine_tag(Some(&s));
        assert_eq!(ex, e);
        assert_eq!(sanitized.as_deref(), Some("alpha+beta"));
        assert_eq!(src, TagSource::Extracted);
    }

    #[test]
    fn derive_engine_tag_when_missing_is_generated_and_original_preserved() {
        let original = "foo+bar";
        let (ex, sanitized, src) = EngineUuid::derive_engine_tag(Some(original));
        assert!(sanitized.as_deref() == Some(original));
        assert_eq!(src, TagSource::Generated);
        // Ensure a valid UUID returned
        let _ = ex.to_uuid();
    }

    #[test]
    fn derive_engine_tag_none_generates_new() {
        let (ex, sanitized, src) = EngineUuid::derive_engine_tag(None);
        assert!(sanitized.is_none());
        assert_eq!(src, TagSource::Generated);
        let _ = ex.to_uuid();
    }

    #[test]
    fn append_engine_tag_when_none_existing() {
        let e = EngineUuid::new();
        let out = EngineUuid::append_engine_tag(None, e);
        assert_eq!(out, tag_str(e));
    }

    #[test]
    fn append_engine_tag_when_existing_without_trailing_delimiter_adds_plus() {
        let e = EngineUuid::new();
        let out = EngineUuid::append_engine_tag(Some("note".to_string()), e);
        assert_eq!(out, format!("note+{}{}", ENGINE_TAG_PREFIX, e.to_hex32()));
    }

    #[test]
    fn append_engine_tag_when_existing_with_trailing_delimiter_preserves_it() {
        let e = EngineUuid::new();
        for delim in ['+', ';', ' '] {
            let base = format!("note{}", delim);
            let out = EngineUuid::append_engine_tag(Some(base.clone()), e);
            assert_eq!(
                out,
                format!("{}{}{}", base, ENGINE_TAG_PREFIX, e.to_hex32())
            );
        }
    }

    #[test]
    fn display_matches_hex32_lowercase() {
        let e = EngineUuid::new();
        assert_eq!(format!("{}", e), e.to_hex32());
        assert!(
            format!("{}", e)
                .chars()
                .all(|c| c.is_ascii_hexdigit() && c.is_ascii_lowercase() || c.is_ascii_digit())
        );
        assert_eq!(format!("{}", e).len(), 32);
    }

    #[test]
    fn from_uuid_and_to_uuid_consistency_for_known_value() {
        let u = Uuid::from_u128(0);
        let e = EngineUuid::from_uuid(u);
        assert_eq!(e.0, 0);
        assert_eq!(e.to_uuid(), u);
        assert_eq!(e.to_hex32(), "0".repeat(32));
    }
}
