use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use strum_macros::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecurityType {
    Future,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, PartialOrd)]
pub enum Exchange {
    CME,
    CBOT,
    COMEX,
    NYMEX,
    GLOBEX,
    EUREX,
    ICEUS,
    ICEEU,
    SGX,
    CFE,
}

impl Exchange {
    #[inline]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "CME" => Some(Exchange::CME),
            "CBOT" => Some(Exchange::CBOT),
            "COMEX" => Some(Exchange::COMEX),
            "NYMEX" => Some(Exchange::NYMEX),
            "GLOBEX" => Some(Exchange::GLOBEX),
            "EUREX" => Some(Exchange::EUREX),
            "ICEUS" => Some(Exchange::ICEUS),
            "ICEEU" => Some(Exchange::ICEEU),
            "SGX" => Some(Exchange::SGX),
            "CFE" => Some(Exchange::CFE),
            _ => None,
        }
    }
    #[inline]
    pub fn map_exchange_bytes(b: &[u8]) -> Option<Exchange> {
        // Match on the bytes to avoid allocating a String.
        // Extend with any other codes you expect from the feed.
        Some(match b {
            b"CME" => Exchange::CME,
            b"GLOBEX" => Exchange::GLOBEX,
            b"CBOT" => Exchange::CBOT,
            b"COMEX" => Exchange::COMEX,
            b"NYMEX" => Exchange::NYMEX,
            b"EUREX" => Exchange::EUREX,
            b"ICEUS" => Exchange::ICEUS,
            b"ICEEU" => Exchange::ICEEU,
            b"SGX" => Exchange::SGX,
            b"CFE" => Exchange::CFE,
            _ => return None,
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
pub enum MarketType {
    Futures,
}
pub fn exchange_market_type(exchange: Exchange) -> MarketType {
    match exchange {
        Exchange::CME
        | Exchange::CBOT
        | Exchange::COMEX
        | Exchange::NYMEX
        | Exchange::GLOBEX
        | Exchange::EUREX
        | Exchange::ICEUS
        | Exchange::ICEEU
        | Exchange::SGX
        | Exchange::CFE => MarketType::Futures,
    }
}

impl FromStr for Exchange {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Exchange::from_str(s).ok_or(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Currency {
    USD,
    EUR,
    GBP,
    AUD,
    JPY,
    TRY,
    USDT,
    Other,
}
impl Currency {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "USD" => Some(Currency::USD),
            "EUR" => Some(Currency::EUR),
            "GBP" => Some(Currency::GBP),
            "AUD" => Some(Currency::AUD),
            "JPY" => Some(Currency::JPY),
            "TRY" => Some(Currency::TRY),
            "USDT" => Some(Currency::USDT),
            _ => None,
        }
    }
}

/// Example canonical: `MNQ.Z25` or continuous contracts `MNQ.C.0`
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Instrument(pub String);

impl Instrument {
    pub const MONTHS: &'static [u8] = b"FGHJKMNQUVXZ";

    pub fn is_valid_month(m: u8) -> bool {
        Self::MONTHS.contains(&m)
    }

    /// 1, 2, or 4-digit year → 1–2 digit suffix
    /// - 4-digit: last two (2025 -> "25")
    /// - 2-digit: keep, but strip leading 0 ("05" -> "5")
    /// - 1-digit: keep
    pub fn normalize_year_digits(digs: &str) -> anyhow::Result<String> {
        if digs.is_empty() || digs.len() > 4 || !digs.chars().all(|c| c.is_ascii_digit()) {
            return Err(anyhow!("invalid year digits: {digs}"));
        }
        let out = match digs.len() {
            1 => digs.to_string(),
            2 => if digs.starts_with('0') { digs[1..].to_string() } else { digs.to_string() },
            4 => digs[2..].to_string(),
            _ => return Err(anyhow!("invalid year length: {}", digs.len())),
        };
        Ok(out)
    }

    /// Build canonical month contract: ROOT.MYY (returns Instrument)
    pub fn build_canonical_month(root: &str, suff: &str) -> anyhow::Result<Self> {
        if suff.len() < 2 {
            return Err(anyhow!("suffix too short: {suff}"));
        }
        let mut chars = suff.chars();
        let m = chars.next().unwrap().to_ascii_uppercase();
        let rest: String = chars.collect();

        if !m.is_ascii_alphabetic() {
            return Err(anyhow!("suffix missing month letter: {suff}"));
        }
        if !Self::is_valid_month(m as u8) {
            return Err(anyhow!("invalid month code: {}", m));
        }
        if root.is_empty() || !root.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(anyhow!("invalid root: {root}"));
        }

        let yy = Self::normalize_year_digits(&rest)?;
        if yy.is_empty() {
            return Err(anyhow!("missing/invalid year digits in: {suff}"));
        }

        let name = format!("{}.{}{}", root.to_ascii_uppercase(), m, yy);
        Self::validate_len(name)
    }

    /// Build canonical continuous contract: ROOT.C.0
    pub fn build_canonical_cont(root: &str) -> anyhow::Result<Self> {
        if root.is_empty() || !root.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(anyhow!("invalid root for continuous: {root}"));
        }
        let name = format!("{}.C.0", root.to_ascii_uppercase());
        Self::validate_len(name)
    }

    /// Enforce a sane upper bound (adjust if you need)
    pub fn validate_len<S: Into<String>>(s: S) -> anyhow::Result<Self> {
        let name = s.into();
        if name.len() > 32 {
            return Err(anyhow!("instrument name too long: {name}"));
        }
        Ok(Instrument(name))
    }

    /// Parse dotted forms, including vendor prefixes and continuous:
    /// - "... MNQ . Z25"  => "MNQ.Z25"
    /// - "... MNQ . c . 0" => "MNQ.C.0"
    pub fn try_parse_dotted(up: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = up.split('.').filter(|p| !p.is_empty()).collect();
        if parts.len() < 2 {
            return Err(anyhow!("not dotted"));
        }

        // continuous detection: last two segments "c" and "0"
        if parts.len() >= 3
            && parts[parts.len() - 2].eq_ignore_ascii_case("c")
            && parts[parts.len() - 1] == "0"
        {
            let root = parts[parts.len() - 3];
            return Self::build_canonical_cont(root);
        }

        // standard month form: last two segments as (root, suff)
        let root = parts[parts.len() - 2];
        let suff = parts[parts.len() - 1];
        Self::build_canonical_month(root, suff)
    }

    /// Parse compact month form like "MNQZ25", "MNQZ5", "NQH6"
    pub fn try_parse_compact(up: &str) -> anyhow::Result<Self> {
        let b = up.as_bytes();
        if b.len() < 3 {
            return Err(anyhow!("too short for compact futures: {up}"));
        }
        let mut i = b.len();

        // collect up to 4 trailing digits
        let mut dstart = i;
        let mut dcount = 0usize;
        while dstart > 0 && b[dstart - 1].is_ascii_digit() && dcount < 4 {
            dstart -= 1;
            dcount += 1;
        }
        if dcount == 0 {
            return Err(anyhow!("no trailing year digits"));
        }
        if dstart == 0 {
            return Err(anyhow!("missing month letter/root"));
        }

        let mpos = dstart - 1;
        let m = b[mpos].to_ascii_uppercase();
        if !Self::is_valid_month(m) {
            return Err(anyhow!("invalid month code in compact: {}", m as char));
        }
        let root = &up[..mpos];
        let digs = &up[dstart..];

        Self::build_canonical_month(root, &format!("{}{}", m as char, digs))
    }
}

impl FromStr for Instrument {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let up = s.trim().to_ascii_uppercase();
        if up.is_empty() {
            return Err(anyhow!("empty instrument string"));
        }

        // 1) dotted (incl. vendor prefixes & continuous c.0)
        if let Ok(inst) = Self::try_parse_dotted(&up) {
            return Ok(inst);
        }

        // 2) compact (e.g., MNQZ25)
        if let Ok(inst) = Self::try_parse_compact(&up) {
            return Ok(inst);
        }

        // 3) explicit already-canonical dotted with odd casing/spacing (redundant but safe)
        if up.contains('.') {
            if let Ok(inst) = {
                let mut it = up.split('.');
                match (it.next(), it.next(), it.next()) {
                    (Some(root), Some(suff), None) => Self::build_canonical_month(root, suff),
                    _ => Err(anyhow!("unrecognized dotted form: {up}")),
                }
            } {
                return Ok(inst);
            }
        }

        Err(anyhow!("could not parse instrument: {s}"))
    }
}
impl Display for Instrument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub fn month_code(month: u32) -> char {
    match month {
        1 => 'F',
        2 => 'G',
        3 => 'H',
        4 => 'J',
        5 => 'K',
        6 => 'M',
        7 => 'N',
        8 => 'Q',
        9 => 'U',
        10 => 'V',
        11 => 'X',
        12 => 'Z',
        _ => '?',
    }
}
pub fn month_from_code(mc: char) -> Option<u32> {
    Some(match mc {
        'F' => 1,
        'G' => 2,
        'H' => 3,
        'J' => 4,
        'K' => 5,
        'M' => 6,
        'N' => 7,
        'Q' => 8,
        'U' => 9,
        'V' => 10,
        'X' => 11,
        'Z' => 12,
        _ => return None,
    })
}

/// Third Friday of a month (typical equity index futures expiry)
pub fn third_friday(year: i32, month: u32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
    let mut d = first;
    let mut fridays = 0;
    loop {
        if d.weekday().number_from_monday() == 5 {
            fridays += 1;
        }
        if fridays == 3 {
            return d;
        }
        d = d.succ_opt().unwrap();
    }
}

#[derive(Debug, Clone)]
pub struct SymbolInfo {
    pub symbol_name: &'static str,
    pub value_per_tick: rust_decimal::Decimal,
    pub tick_size: rust_decimal::Decimal,
    pub decimal_accuracy: u32,
    pub exchange: Exchange,
}

// Static array of all symbol info
const SYMBOL_INFO_PAIRS: &[(&str, SymbolInfo)] = &[
    (
        "XC",
        SymbolInfo {
            symbol_name: "XC",
            value_per_tick: dec!(1.25),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "XK",
        SymbolInfo {
            symbol_name: "XK",
            value_per_tick: dec!(1.25),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "XW",
        SymbolInfo {
            symbol_name: "XW",
            value_per_tick: dec!(1.25),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "YM",
        SymbolInfo {
            symbol_name: "YM",
            value_per_tick: dec!(5.0),
            tick_size: dec!(1.0),
            decimal_accuracy: 0,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "ZB",
        SymbolInfo {
            symbol_name: "ZB",
            value_per_tick: dec!(1.953125),
            tick_size: dec!(0.0625),
            decimal_accuracy: 4,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "ZC",
        SymbolInfo {
            symbol_name: "ZC",
            value_per_tick: dec!(12.5),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "ZF",
        SymbolInfo {
            symbol_name: "ZF",
            value_per_tick: dec!(0.244140625),
            tick_size: dec!(0.0078125),
            decimal_accuracy: 6,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "ZL",
        SymbolInfo {
            symbol_name: "ZL",
            value_per_tick: dec!(6.0),
            tick_size: dec!(0.01),
            decimal_accuracy: 4,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "ZM",
        SymbolInfo {
            symbol_name: "ZM",
            value_per_tick: dec!(10.0),
            tick_size: dec!(0.1),
            decimal_accuracy: 1,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "ZN",
        SymbolInfo {
            symbol_name: "ZN",
            value_per_tick: dec!(0.48828125),
            tick_size: dec!(0.015625),
            decimal_accuracy: 5,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "ZO",
        SymbolInfo {
            symbol_name: "ZO",
            value_per_tick: dec!(12.5),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "ZR",
        SymbolInfo {
            symbol_name: "ZR",
            value_per_tick: dec!(0.25),
            tick_size: dec!(0.005),
            decimal_accuracy: 4,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "ZS",
        SymbolInfo {
            symbol_name: "ZS",
            value_per_tick: dec!(12.5),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "ZT",
        SymbolInfo {
            symbol_name: "ZT",
            value_per_tick: dec!(0.244140625),
            tick_size: dec!(0.0078125),
            decimal_accuracy: 6,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "ZW",
        SymbolInfo {
            symbol_name: "ZW",
            value_per_tick: dec!(12.5),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CBOT,
        },
    ),
    //
    (
        "6A",
        SymbolInfo {
            symbol_name: "6A",
            value_per_tick: dec!(1.0),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "6B",
        SymbolInfo {
            symbol_name: "6B",
            value_per_tick: dec!(0.625),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "6C",
        SymbolInfo {
            symbol_name: "6C",
            value_per_tick: dec!(1.0),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "6E",
        SymbolInfo {
            symbol_name: "6E",
            value_per_tick: dec!(1.25),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "6J",
        SymbolInfo {
            symbol_name: "6J",
            value_per_tick: dec!(0.0125),
            tick_size: dec!(0.000001),
            decimal_accuracy: 6,
            exchange: Exchange::CME,
        },
    ),
    (
        "6M",
        SymbolInfo {
            symbol_name: "6M",
            value_per_tick: dec!(0.1),
            tick_size: dec!(0.00001),
            decimal_accuracy: 5,
            exchange: Exchange::CME,
        },
    ),
    (
        "6N",
        SymbolInfo {
            symbol_name: "6N",
            value_per_tick: dec!(1.0),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "6S",
        SymbolInfo {
            symbol_name: "6S",
            value_per_tick: dec!(1.25),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "E7",
        SymbolInfo {
            symbol_name: "E7",
            value_per_tick: dec!(0.625),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "EMD",
        SymbolInfo {
            symbol_name: "EM",
            value_per_tick: dec!(2.5),
            tick_size: dec!(0.05),
            decimal_accuracy: 2,
            exchange: Exchange::CME,
        },
    ),
    (
        "ES",
        SymbolInfo {
            symbol_name: "ES",
            value_per_tick: dec!(12.5),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CME,
        },
    ),
    (
        "GE",
        SymbolInfo {
            symbol_name: "GE",
            value_per_tick: dec!(0.0625),
            tick_size: dec!(0.0025),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "GF",
        SymbolInfo {
            symbol_name: "GF",
            value_per_tick: dec!(1.25),
            tick_size: dec!(0.025),
            decimal_accuracy: 3,
            exchange: Exchange::CME,
        },
    ),
    (
        "HE",
        SymbolInfo {
            symbol_name: "HE",
            value_per_tick: dec!(0.1),
            tick_size: dec!(0.0025),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "J7",
        SymbolInfo {
            symbol_name: "J7",
            value_per_tick: dec!(0.00625),
            tick_size: dec!(0.000001),
            decimal_accuracy: 6,
            exchange: Exchange::CME,
        },
    ),
    (
        "LE",
        SymbolInfo {
            symbol_name: "LE",
            value_per_tick: dec!(1.0),
            tick_size: dec!(0.025),
            decimal_accuracy: 3,
            exchange: Exchange::CME,
        },
    ),
    (
        "NQ",
        SymbolInfo {
            symbol_name: "NQ",
            value_per_tick: dec!(5.0),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CME,
        },
    ),
    (
        "RF",
        SymbolInfo {
            symbol_name: "RF",
            value_per_tick: dec!(1.25),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "SP",
        SymbolInfo {
            symbol_name: "SP",
            value_per_tick: dec!(25.0),
            tick_size: dec!(0.1),
            decimal_accuracy: 2,
            exchange: Exchange::CME,
        },
    ),
    // COMEX Futures
    (
        "GC",
        SymbolInfo {
            symbol_name: "GC",
            value_per_tick: dec!(10.0),
            tick_size: dec!(0.1),
            decimal_accuracy: 2,
            exchange: Exchange::COMEX,
        },
    ),
    (
        "HG",
        SymbolInfo {
            symbol_name: "HG",
            value_per_tick: dec!(0.0125),
            tick_size: dec!(0.0005),
            decimal_accuracy: 4,
            exchange: Exchange::COMEX,
        },
    ),
    (
        "QI",
        SymbolInfo {
            symbol_name: "QI",
            value_per_tick: dec!(0.03125),
            tick_size: dec!(0.0025),
            decimal_accuracy: 4,
            exchange: Exchange::COMEX,
        },
    ),
    (
        "SI",
        SymbolInfo {
            symbol_name: "SI",
            value_per_tick: dec!(0.125),
            tick_size: dec!(0.005),
            decimal_accuracy: 3,
            exchange: Exchange::COMEX,
        },
    ),
    // NYMEX Futures
    (
        "CL",
        SymbolInfo {
            symbol_name: "CL",
            value_per_tick: dec!(10.0),
            tick_size: dec!(0.01),
            decimal_accuracy: 2,
            exchange: Exchange::NYMEX,
        },
    ),
    (
        "HO",
        SymbolInfo {
            symbol_name: "HO",
            value_per_tick: dec!(4.2),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::NYMEX,
        },
    ),
    (
        "NG",
        SymbolInfo {
            symbol_name: "NG",
            value_per_tick: dec!(10.0),
            tick_size: dec!(0.001),
            decimal_accuracy: 3,
            exchange: Exchange::NYMEX,
        },
    ),
    (
        "PA",
        SymbolInfo {
            symbol_name: "PA",
            value_per_tick: dec!(5.0),
            tick_size: dec!(0.05),
            decimal_accuracy: 2,
            exchange: Exchange::NYMEX,
        },
    ),
    (
        "PL",
        SymbolInfo {
            symbol_name: "PL",
            value_per_tick: dec!(5.0),
            tick_size: dec!(0.1),
            decimal_accuracy: 2,
            exchange: Exchange::NYMEX,
        },
    ),
    (
        "QM",
        SymbolInfo {
            symbol_name: "QM",
            value_per_tick: dec!(5.0),
            tick_size: dec!(0.01),
            decimal_accuracy: 2,
            exchange: Exchange::NYMEX,
        },
    ),
    (
        "RB",
        SymbolInfo {
            symbol_name: "RB",
            value_per_tick: dec!(4.2),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::NYMEX,
        },
    ),
    // Micro Futures
    (
        "MES",
        SymbolInfo {
            symbol_name: "MES",
            value_per_tick: dec!(1.25),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CME,
        },
    ),
    (
        "MNQ",
        SymbolInfo {
            symbol_name: "MNQ",
            value_per_tick: dec!(0.50),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CME,
        },
    ),
    (
        "M2K",
        SymbolInfo {
            symbol_name: "M2K",
            value_per_tick: dec!(0.50),
            tick_size: dec!(0.1),
            decimal_accuracy: 2,
            exchange: Exchange::CME,
        },
    ),
    (
        "MYM",
        SymbolInfo {
            symbol_name: "MYM",
            value_per_tick: dec!(0.50),
            tick_size: dec!(1.0),
            decimal_accuracy: 0,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "MGC",
        SymbolInfo {
            symbol_name: "MGC",
            value_per_tick: dec!(1.0),
            tick_size: dec!(0.1),
            decimal_accuracy: 2,
            exchange: Exchange::COMEX,
        },
    ),
    (
        "SIL",
        SymbolInfo {
            symbol_name: "SIL",
            value_per_tick: dec!(0.0125),
            tick_size: dec!(0.005),
            decimal_accuracy: 3,
            exchange: Exchange::COMEX,
        },
    ),
    (
        "MCL",
        SymbolInfo {
            symbol_name: "MCL",
            value_per_tick: dec!(1.0),
            tick_size: dec!(0.01),
            decimal_accuracy: 2,
            exchange: Exchange::NYMEX,
        },
    ),
    (
        "MBT",
        SymbolInfo {
            symbol_name: "MBT",
            value_per_tick: dec!(1.25),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CME,
        },
    ),
    (
        "M6A",
        SymbolInfo {
            symbol_name: "M6A",
            value_per_tick: dec!(0.1),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "M6B",
        SymbolInfo {
            symbol_name: "M6B",
            value_per_tick: dec!(0.0625),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "M6E",
        SymbolInfo {
            symbol_name: "M6E",
            value_per_tick: dec!(0.125),
            tick_size: dec!(0.0001),
            decimal_accuracy: 4,
            exchange: Exchange::CME,
        },
    ),
    (
        "MJY",
        SymbolInfo {
            symbol_name: "MJY",
            value_per_tick: dec!(0.00125),
            tick_size: dec!(0.000001),
            decimal_accuracy: 6,
            exchange: Exchange::CME,
        },
    ),
    (
        "MET",
        SymbolInfo {
            symbol_name: "MET",
            value_per_tick: dec!(1.25),
            tick_size: dec!(0.25),
            decimal_accuracy: 2,
            exchange: Exchange::CME,
        },
    ),
    (
        "MHG",
        SymbolInfo {
            symbol_name: "MHG",
            value_per_tick: dec!(0.0025),
            tick_size: dec!(0.0005),
            decimal_accuracy: 4,
            exchange: Exchange::COMEX,
        },
    ),
    (
        "MNG",
        SymbolInfo {
            symbol_name: "MNG",
            value_per_tick: dec!(2.5),
            tick_size: dec!(0.001),
            decimal_accuracy: 3,
            exchange: Exchange::NYMEX,
        },
    ),
    (
        "NKD",
        SymbolInfo {
            symbol_name: "NKD",
            value_per_tick: dec!(5.0),
            tick_size: dec!(5.0),
            decimal_accuracy: 0,
            exchange: Exchange::CME,
        },
    ),
    (
        "QG",
        SymbolInfo {
            symbol_name: "QG",
            value_per_tick: dec!(12.5),
            tick_size: dec!(0.0025),
            decimal_accuracy: 4,
            exchange: Exchange::NYMEX,
        },
    ),
    (
        "RTY",
        SymbolInfo {
            symbol_name: "RTY",
            value_per_tick: dec!(5.0),
            tick_size: dec!(0.1),
            decimal_accuracy: 2,
            exchange: Exchange::CME,
        },
    ),
    (
        "TN",
        SymbolInfo {
            symbol_name: "TN",
            value_per_tick: dec!(15.625),
            tick_size: dec!(0.015625),
            decimal_accuracy: 5,
            exchange: Exchange::CBOT,
        },
    ),
    (
        "UB",
        SymbolInfo {
            symbol_name: "UB",
            value_per_tick: dec!(31.25),
            tick_size: dec!(0.03125),
            decimal_accuracy: 5,
            exchange: Exchange::CBOT,
        },
    ),
];

use ahash::AHashMap;
use anyhow::anyhow;
use once_cell::sync::Lazy;
use rust_decimal::dec;

static SYMBOL_INFO_MAP: Lazy<AHashMap<&'static str, SymbolInfo>> = Lazy::new(|| {
    let mut map = AHashMap::with_capacity(SYMBOL_INFO_PAIRS.len());
    for (k, v) in SYMBOL_INFO_PAIRS.iter() {
        map.insert(*k, v.clone());
    }
    map
});

pub fn get_symbol_info(symbol: &str) -> Option<&SymbolInfo> {
    SYMBOL_INFO_MAP.get(symbol)
}
