// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::borrow::Cow;

use chrono::{Datelike, Duration, Months, NaiveDate, NaiveTime, Utc};
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    instruments::FuturesContract,
};

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
pub fn activation_ns_from_code_with_policy(code: &str, policy: ActivationPolicy) -> Option<u64> {
    let expiry_month_start = parse_expiry_from_contract_code(code)?;
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
pub fn activation_ns_default(root: &str, code: &str) -> Option<u64> {
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

    activation_ns_from_code_with_policy(code, policy)
}

pub fn parse_expiry_from_contract_code(code: &str) -> Option<NaiveDate> {
    let s = sanitize_code(code).to_ascii_uppercase();
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
pub(crate) fn extract_root(code: &str) -> String {
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

// Main entrypoint: pass "M6AU5" -> FuturesContract
pub fn build_futures_from_code(id: InstrumentId, code: &str) -> Option<FuturesContract> {
    let root = extract_root(code);
    let specs = crate::common::root_specs::hardcoded_roots();
    let spec = specs.get(root.as_str())?;

    let raw_symbol = Symbol::from_ustr_unchecked(code.into());

    let expiry_date = parse_expiry_from_contract_code(code)?;
    let expiration_ns: u64 = unix_nanos_utc_from_date(expiry_date);
    let ts_event: u64 = Utc::now().timestamp_nanos_opt()? as u64;
    let activation_ns: u64 = activation_ns_default(root.as_str(), code)?;

    match FuturesContract::new_checked(
        id,
        raw_symbol,
        spec.asset_class,
        spec.exchange,
        spec.underlying,
        UnixNanos::new(activation_ns),
        UnixNanos::new(expiration_ns),
        spec.currency,
        spec.price_precision,
        spec.price_increment,
        spec.multiplier,
        spec.size_increment,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UnixNanos::new(ts_event),
        UnixNanos::new(ts_event),
    ) {
        Ok(c) => Some(c),
        Err(e) => {
            log::error!("Failed to build futures contract: {}", e);
            None
        }
    }
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
