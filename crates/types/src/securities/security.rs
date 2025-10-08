use crate::providers::ProviderKind;
use crate::securities::futures_helpers::{
    activation_ns_default, extract_root, parse_expiry_from_instrument,
};
use crate::securities::symbols::{Currency, Exchange, Instrument, SecurityType, get_symbol_info};
use chrono::{DateTime, NaiveDate, Utc};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;

/// Handle that composes facts (props), calendar (hours), models, and small runtime cache.
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Eq, PartialEq, Clone, Ord, PartialOrd, Debug)]
#[archive(check_bytes)]
pub struct FuturesContract {
    pub root: String,
    pub instrument: Instrument,
    pub security_type: SecurityType,
    pub exchange: Exchange,
    /// Provider ID for this instrument, this is the id used to place orders on the exchange.
    pub provider_contract_name: String,
    pub provider_id: ProviderKind,

    pub tick_size: Decimal,

    pub value_per_tick: Decimal,
    pub quote_ccy: Currency,

    pub activation_date: NaiveDate,

    pub expiration_date: NaiveDate,
    pub is_continuous: bool,
}

impl FuturesContract {
    pub fn from_root_with(
        instrument: &Instrument,
        exchange: Exchange,
        security_type: SecurityType,
        provider_contract_name: String,
        provider_id: ProviderKind,
        value_per_tick: Decimal,
        quote_ccy: Option<Currency>,
        tick_size: Decimal,
    ) -> Option<Self> {
        let root = extract_root(instrument);
        let is_continuous = root == instrument.to_string();
        let (expiry, activation) = match is_continuous {
            true => (NaiveDate::MAX, NaiveDate::MIN),
            false => {
                let expiry = parse_expiry_from_instrument(&instrument)?;
                let activation_ns = activation_ns_default(&root, instrument)?;
                let activation =
                    DateTime::<Utc>::from_timestamp_nanos(activation_ns as i64).date_naive();
                (expiry, activation)
            }
        };
        let quote_ccy = match quote_ccy {
            None => Currency::USD,
            Some(c) => c
        };
        Some(Self {
            root,
            provider_contract_name,
            provider_id,
            instrument: instrument.clone(),
            security_type,
            exchange,
            tick_size,
            value_per_tick,
            quote_ccy,
            activation_date: activation,
            expiration_date: expiry,
            is_continuous,
        })
    }
    pub fn from_root_with_serialized(
        instrument: &Instrument,
        exchange: Exchange,
        security_type: SecurityType,
        provider_contract_name: String,
        provider_id: ProviderKind,
    ) -> Option<Self> {
        let root = extract_root(instrument);
        let is_continuous = root == instrument.to_string();
        let binding = root.clone();
        let symbol_info = get_symbol_info(binding.as_str())?;
        let (expiry, activation) = match is_continuous {
            true => (NaiveDate::MAX, NaiveDate::MIN),
            false => {
                let expiry = parse_expiry_from_instrument(&instrument)?;
                let activation_ns = activation_ns_default(&root, instrument)?;
                let activation =
                    DateTime::<Utc>::from_timestamp_nanos(activation_ns as i64).date_naive();
                (expiry, activation)
            }
        };

        Some(Self {
            root,
            provider_contract_name,
            provider_id,
            instrument: instrument.clone(),
            security_type,
            exchange,
            tick_size: symbol_info.tick_size,
            value_per_tick: symbol_info.value_per_tick,
            quote_ccy: Currency::USD,
            activation_date: activation,
            expiration_date: expiry,
            is_continuous,
        })
    }
}
