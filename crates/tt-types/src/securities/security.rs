use crate::providers::ProviderKind;
use crate::securities::futures_helpers::{
    activation_ns_default, extract_root, parse_expiry_from_instrument,
};
use crate::securities::market_hours::{MarketHours, hours_for_exchange};
use crate::securities::symbols::{Currency, Exchange, Instrument, SecurityType, get_symbol_info};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

/// Handle that composes facts (props), calendar (hours), models, and small runtime cache.
#[derive(Clone, Debug)]
pub struct FuturesContract {
    pub root: String,
    pub instrument: Instrument,
    pub security_type: SecurityType,
    pub exchange: Exchange,
    /// Provider ID for this instrument, this is the id used to place orders on the exchange.
    pub provider_contract_name: String,
    pub provider_id: ProviderKind,
    /// Trading calendar for this instrument
    pub hours: MarketHours,

    /*    /// Pluggable models (always present; defaulted by an initializer/factory)
    pub fee_model: Arc<dyn FeeModel>,
    pub slippage_model: Arc<dyn SlippageModel>,
    pub fill_model: Arc<dyn FillModel>,
    pub bp_model: Arc<dyn BuyingPowerModel>,
    pub vol_model: Arc<dyn VolatilityModel>,
    pub settlement_model: Arc<dyn SettlementModel>,*/
    pub tick_size: Decimal,
    pub value_per_tick: Decimal,
    pub decimal_accuracy: u32,
    pub quote_ccy: Currency,
    pub activation_date: Option<NaiveDate>,
    pub expiration_date: Option<NaiveDate>,
    pub is_continuous: bool,
}

impl FuturesContract {
    pub fn from_root_with_default_models(
        instrument: &Instrument,
        exchange: Exchange,
        security_type: SecurityType,
        provider_contract_name: String,
        provider_id: ProviderKind,
    ) -> Option<Self> {
        let root = extract_root(instrument);
        let market_hours = hours_for_exchange(exchange);
        let is_continuous = root == instrument.to_string();
        let binding = root.clone();
        let symbol_info = get_symbol_info(binding.as_str())?;
        let (expiry, activation) = match is_continuous {
            true => (None, None),
            false => {
                let expiry = parse_expiry_from_instrument(&instrument)?;
                let activation_ns = activation_ns_default(&root, instrument)?;
                let activation =
                    DateTime::<Utc>::from_timestamp_nanos(activation_ns as i64).date_naive();
                (Some(expiry), Some(activation))
            }
        };

        Some(Self {
            root,
            provider_contract_name,
            provider_id,
            instrument: instrument.clone(),
            security_type,
            exchange,
            hours: market_hours,
            tick_size: symbol_info.tick_size,
            value_per_tick: symbol_info.value_per_tick,
            decimal_accuracy: symbol_info.decimal_accuracy,
            quote_ccy: Currency::USD,
            activation_date: activation,
            expiration_date: expiry,
            is_continuous,
        })
    }
}
