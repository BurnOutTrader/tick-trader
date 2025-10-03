use std::fmt::Display;
use std::str::FromStr;
use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use strum_macros::Display;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecurityType {
    Future,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
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


///Example: `MNQZ5`
#[derive(Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Instrument(String);

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
