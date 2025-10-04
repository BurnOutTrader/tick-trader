use std::borrow::Cow;
use chrono::{Datelike, Duration, Months, NaiveDate, NaiveTime};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use crate::base_data::Price;
use crate::securities::symbols::Instrument;

#[inline]
fn sanitize_code(s: &str) -> Cow<'_, str> {
    if s.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Cow::Borrowed(s);
    }
    Cow::Owned(s.chars().filter(|c| c.is_ascii_alphanumeric()).collect())
}

#[inline]
fn month_from_code(c: char) -> Option<u32> {
    Some(match c {
        'F' => 1,  // Jan
        'G' => 2,  // Feb
        'H' => 3,  // Mar
        'J' => 4,  // Apr
        'K' => 5,  // May
        'M' => 6,  // Jun
        'N' => 7,  // Jul
        'Q' => 8,  // Aug
        'U' => 9,  // Sep
        'V' => 10, // Oct
        'X' => 11, // Nov
        'Z' => 12, // Dec
        _ => return None,
    })
}

// ==== activation policy & calculators ====

/// How to derive activation time from the parsed expiry month.
#[derive(Clone, Copy, Debug)]
pub enum ActivationPolicy {
    /// Activation is `N` calendar months before the **first day of the expiry month** (00:00:00Z).
    MonthsBefore(u32),
    /// Activation is `N` calendar days before the **first day of the expiry month** (00:00:00Z).
    DaysBefore(u32),
}

// ==== timestamp utils ====

#[inline]
fn unix_nanos_midnight_utc(date: NaiveDate) -> u64 {
    let dt = date.and_time(NaiveTime::MIN).and_utc();
    // Compose from (secs, nanos) and check range to avoid negative → u64 UB
    let secs: i64 = dt.timestamp();
    let subnanos: u32 = dt.timestamp_subsec_nanos();
    let total_ns = (secs as i128) * 1_000_000_000i128 + (subnanos as i128);
    u64::try_from(total_ns).expect("timestamp before 1970-01-01 or overflow")
}

/// Compute activation (u64 ns) from a contract code with a chosen policy.
/// Returns `None` if the code cannot be parsed.
pub fn activation_ns_from_code_with_policy(instrument: &Instrument, policy: ActivationPolicy) -> Option<u64> {
    let expiry_month_start = parse_expiry_from_instrument(&instrument)?;
    let activation_date = match policy {
        ActivationPolicy::MonthsBefore(m) => {
            expiry_month_start.checked_sub_months(Months::new(m))?
        }
        ActivationPolicy::DaysBefore(d) => {
            // FIX: use Duration::days instead of Days::new with checked_sub_signed
            expiry_month_start.checked_sub_signed(Duration::days(d as i64))?
            // alternatively:
            // expiry_month_start.checked_sub_days(chrono::Days::new(d as u64))?
        }
    };
    Some(unix_nanos_midnight_utc(activation_date))
}

/// Convenience: choose a reasonable default per **root symbol** family.
/// - FX & Equity Index (incl. micros): 3 months before
/// - Treasuries: 2 months before
/// - Monthly commodities (energies, metals, ags, livestock, crypto): 1 month before
///
/// You can tweak this mapping to your venue rules without touching call sites.
pub fn activation_ns_default(root: &str, instrument: &Instrument) -> Option<u64> {
    let r = root.to_ascii_uppercase();

    // FX & minis
    const FX_ROOTS: [&str; 12] = [
        "6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S", "E7", "M6A", "M6B", "M6E",
    ];
    // Equity indices
    const IDX_ROOTS: [&str; 8] = ["ES", "NQ", "RTY", "YM", "MNQ", "MES", "MYM", "M2K"];
    // Treasuries
    const TR_ROOTS: [&str; 6] = ["ZT", "ZF", "ZN", "ZB", "UB", "TN"];

    let policy = if FX_ROOTS.iter().any(|&k| k == r) || IDX_ROOTS.iter().any(|&k| k == r) {
        ActivationPolicy::MonthsBefore(3)
    } else if TR_ROOTS.iter().any(|&k| k == r) {
        ActivationPolicy::MonthsBefore(2)
    } else {
        // Metals, energies, ags, livestock, crypto → monthly listings
        ActivationPolicy::MonthsBefore(1)
    };

    activation_ns_from_code_with_policy(instrument, policy)
}

pub fn parse_expiry_from_instrument(instrument: &Instrument) -> Option<NaiveDate> {
    let code = instrument.to_string();
    let s = sanitize_code(&code).to_ascii_uppercase();
    let bytes = s.as_bytes();

    // 1) Collect 1–2 trailing digits as the year
    let mut i = bytes.len();
    let mut year_digits = 0usize;
    while i > 0 && bytes[i - 1].is_ascii_digit() && year_digits < 2 {
        i -= 1;
        year_digits += 1;
    }
    if year_digits == 0 {
        return None; // no trailing year
    }

    // 2) The character before the digits must be the month letter
    if i == 0 {
        return None; // there was no month letter before digits
    }
    let month_char = s.chars().nth(i - 1)?; // preceding char
    let month = month_from_code(month_char)?;

    // 3) Parse year (1- or 2-digit)
    let year_str = &s[i..];
    let now = chrono::Utc::now().date_naive();
    let year = if year_digits == 1 {
        // Single-digit year → pivot to current decade, roll forward if needed
        let d = year_str.chars().next()?.to_digit(10)? as i32;
        let mut y = (now.year() / 10) * 10 + d; // e.g., 2025 → 2025 when d=5
        if y < now.year() - 2 {
            y += 10;
        } // avoid picking too far in the past
        y
    } else {
        // Two-digit year. Assume 2000..2079 to cover current trading horizons.
        // (If you want broader, choose a different pivot.)
        let yy = year_str.parse::<i32>().ok()?;
        if yy <= 79 { 2000 + yy } else { 1900 + yy }
    };

    NaiveDate::from_ymd_opt(year, month, 1)
}


#[allow(dead_code)]
/// Extract a futures **root** from a vendor symbol code.
///
/// Heuristic:
/// - Uppercase the input.
/// - Strip **up to two trailing digits** (year suffix).
/// - If there was at least one digit removed and the prior char is an
///   alphabetic **month code** (F,G,H, …, Z), strip that as well.
/// - Return everything **before** that boundary.
///
/// This produces:
/// - `"ESZ25"` → `"ES"`
/// - `"MNQZ5"` → `"MNQ"`
/// - `"CL"`    → `"CL"` (no change)
///
/// The function is intentionally fast and allocation-light; it does not
/// validate that the remaining characters are a real exchange root.
///
/// ### Notes
/// - Works for common futures codes with 1–2 digit years.
/// - If you need 3–4 digit year formats, extend the heuristic.
///
/// ### Example
/// ```ignore
/// assert_eq!(extract_root("ESZ25"), "ES");
/// assert_eq!(extract_root("MNQZ5"), "MNQ");
/// assert_eq!(extract_root("cl"), "CL");
pub fn extract_root(instrument: &Instrument) -> String {
    let code = instrument.to_string();
    let up = code.to_ascii_uppercase();
    let b = up.as_bytes();
    let mut i = b.len();
    let mut digits = 0;
    while i > 0 && b[i - 1].is_ascii_digit() && digits < 2 {
        i -= 1;
        digits += 1;
    }
    if i > 0 && digits > 0 && b[i - 1].is_ascii_alphabetic() {
        i -= 1;
    }
    code[..i].to_string()
}

#[allow(dead_code)]
fn unix_nanos_utc_from_date(date: NaiveDate) -> u64 {
    // 00:00:00 UTC on that date
    let dt = date.and_time(NaiveTime::MIN).and_utc(); // DateTime<Utc>

    // Safer than casting timestamp_nanos directly
    let secs: i64 = dt.timestamp(); // seconds since Unix epoch
    let subnanos: u32 = dt.timestamp_subsec_nanos(); // 0..1_000_000_000

    // Compose as i128, then checked-convert to u64
    let total_ns_i128 = (secs as i128) * 1_000_000_000i128 + (subnanos as i128);
    u64::try_from(total_ns_i128).expect("timestamp before 1970-01-01 or overflowed u64")
}

#[inline]
pub fn parse_symbol_from_contract_id(contract_id: &str) -> Option<String> {
    if !contract_id.starts_with("CON.") {
        return None;
    }
    let parts: Vec<&str> = contract_id.split('.').collect();
    if parts.len() >= 4 {
        // Compose the 2-part symbol as per requirement: ROOT.CODE
        Some(format!("{}.{}", parts[2], parts[3]))
    } else {
        None
    }
}

#[inline]
pub fn round_to_decimals(value: Decimal, decimals: u32) -> Price {
    // Create a factor of 10^decimals using Decimal
    let factor = Decimal::from_i64(10_i64.pow(decimals)).unwrap();

    // Perform the rounding operation
    (value * factor).round() / factor
}

#[inline]
pub fn round_to_tick_size(value: Decimal, tick_size: Decimal) -> Price {
    // Divide the value by the tick size, then round to the nearest integer
    let ticks = (value / tick_size).round();

    // Multiply the rounded number of ticks by the tick size to get_requests the final rounded value
    ticks * tick_size
}

/// Extract the futures month letter and year code (1–2 digits) from an instrument code.
///
/// This does not assume whether the root is 2 or 3 characters. It scans from the end
/// for 1–2 trailing digits (year), then takes the preceding character as the month code
/// and validates it against standard futures month letters (F,G,H,J,K,M,N,Q,U,V,X,Z).
///
/// Examples:
/// - "ESZ25" -> Some(('Z', 25))
/// - "MNQZ5" -> Some(('Z', 5))
/// - "HEG24" -> Some(('G', 24))
/// - "CL"    -> None
#[inline]
pub fn extract_month_year(instrument: &Instrument) -> Option<(char, u8)> {
    let code = instrument.to_string();
    let s = sanitize_code(&code).to_ascii_uppercase();
    let b = s.as_bytes();
    let mut i = b.len();
    let mut year_digits = 0usize;
    while i > 0 && b[i - 1].is_ascii_digit() && year_digits < 2 {
        i -= 1;
        year_digits += 1;
    }
    if year_digits == 0 || i == 0 { return None; }

    // Preceding char is the month
    let month_ch = s.as_bytes()[i - 1] as char;
    // Validate month
    if month_from_code(month_ch).is_none() { return None; }

    // Parse year (keep as 1–2 digit code, not a full year)
    let year_slice = &s[i..];
    if year_slice.is_empty() || year_slice.len() > 2 { return None; }
    let year_u8 = year_slice.parse::<u8>().ok()?;
    Some((month_ch, year_u8))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::securities::symbols::Instrument;

    #[test]
    fn test_extract_month_year_two_letter_root() {
        let i = Instrument::try_from("ESZ25").expect("ESZ25");
        assert_eq!(extract_month_year(&i), Some(('Z', 25)));
    }

    #[test]
    fn test_extract_month_year_three_letter_root() {
        let i = Instrument::try_from("MNQZ5").expect("MNQZ5");
        assert_eq!(extract_month_year(&i), Some(('Z', 5)));
    }

    #[test]
    fn test_extract_month_year_other() {
        let i = Instrument::try_from("HEG24").expect("HEG24");
        assert_eq!(extract_month_year(&i), Some(('G', 24)));
    }

    #[test]
    fn test_extract_month_year_none() {
        let i = Instrument::try_from("CL").expect("CL");
        assert_eq!(extract_month_year(&i), None);
    }
}
