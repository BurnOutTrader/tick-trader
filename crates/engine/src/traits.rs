use crate::handle::EngineHandle;
use crate::models::DataTopic;
use tt_types::accounts::events::AccountDelta;
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::AccountKey;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{OrdersBatch, PositionsBatch, Trade};

pub trait Strategy: Send + 'static {
    fn on_start(&mut self, _h: EngineHandle) {}
    fn on_warmup_complete(&mut self) {}
    fn on_stop(&mut self) {}

    fn on_tick(&mut self, _t: &Tick, _provider_kind: ProviderKind) {}
    fn on_quote(&mut self, _q: &Bbo, _provider_kind: ProviderKind) {}
    fn on_bar(&mut self, _b: &Candle, _provider_kind: ProviderKind) {}
    fn on_mbp10(&mut self, _d: &Mbp10, _provider_kind: ProviderKind) {}

    fn on_orders_batch(&mut self, _b: &OrdersBatch) {}
    fn on_positions_batch(&mut self, _b: &PositionsBatch) {}
    fn on_account_delta(&mut self, _accounts: &[AccountDelta]) {}

    fn on_trades_closed(&mut self, _trades: Vec<Trade>) {}

    fn on_subscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic, _success: bool) {}
    fn on_unsubscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic) {}

    fn accounts(&self) -> Vec<AccountKey> {
        Vec::new()
    }
}
