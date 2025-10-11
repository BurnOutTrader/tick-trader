use crate::data::core::{Bbo, Candle, Tick, TickBar};
use crate::data::models::{Resolution, TradeSide};
use crate::securities::hours::market_hours::{MarketHours, next_session_after, session_bounds};
use crate::securities::symbols::Instrument;
use chrono::{DateTime, Duration, TimeZone, Utc};
use rust_decimal::{Decimal, dec};
use std::sync::Arc;
// ===================== Helpers =====================

fn end_inclusive(end: DateTime<Utc>) -> DateTime<Utc> {
    end - Duration::nanoseconds(1)
}

#[allow(unused)]
/// Key for ordering `Resolution` by time scale, not declaration order.
/// Lower key => "finer" timeframe. We only compare time-based frames here.
/// TickBars are not compared here (handled separately).
fn resolution_key(r: &Resolution) -> Option<(u8, u64)> {
    match r {
        Resolution::Seconds(s) => Some((1, *s as u64)),
        Resolution::Minutes(m) => Some((2, *m as u64)),
        Resolution::Hours(h) => Some((3, *h as u64)),
        Resolution::Daily => Some((4, 1)),
        Resolution::Weekly => Some((5, 1)),
    }
}

fn fixed_len(res: &Resolution) -> Duration {
    match res {
        Resolution::Seconds(s) => Duration::seconds(*s as i64),
        Resolution::Minutes(m) => Duration::minutes(*m as i64),
        Resolution::Hours(h) => Duration::hours(*h as i64),
        // Daily/Weekly handled by sessions; others default
        _ => Duration::seconds(1),
    }
}

fn floor_to(res: &Resolution, t: DateTime<Utc>) -> DateTime<Utc> {
    match res {
        Resolution::Seconds(s) => {
            let step = *s as i64;
            let floored = t.timestamp() - t.timestamp().rem_euclid(step);
            Utc.timestamp_opt(floored, 0).unwrap()
        }
        Resolution::Minutes(m) => {
            let step = (*m as i64) * 60;
            let floored = t.timestamp() - t.timestamp().rem_euclid(step);
            Utc.timestamp_opt(floored, 0).unwrap()
        }
        Resolution::Hours(h) => {
            let step = (*h as i64) * 3600;
            let floored = t.timestamp() - t.timestamp().rem_euclid(step);
            Utc.timestamp_opt(floored, 0).unwrap()
        }
        _ => t,
    }
}

// ===================== Consolidators (pull-based) =====================

// A unified output type for engine-facing consolidators
#[derive(Debug, Clone)]
pub enum ConsolidatedOut {
    Candle(Candle),
    TickBar(TickBar),
}

// An object-safe trait so different consolidators can be stored uniformly
// and driven by the engine using the same interface.
pub trait Consolidator: Send {
    fn on_tick(&mut self, _tk: &Tick) -> Option<ConsolidatedOut> {
        None
    }
    fn on_bbo(&mut self, _bbo: &Bbo) -> Option<ConsolidatedOut> {
        None
    }
    fn on_candle(&mut self, _bar: &Candle) -> Option<ConsolidatedOut> {
        None
    }
    fn on_time(&mut self, _t: DateTime<Utc>) -> Option<ConsolidatedOut> {
        None
    }
}

pub struct TicksToTickBarsConsolidator {
    ticks_per_bar: u32,
    out_symbol: String,
    instrument: Instrument,
    // state
    count: u32,
    start_ts: Option<DateTime<Utc>>,
    o: Option<Decimal>,
    h: Option<Decimal>,
    l: Option<Decimal>,
    c: Option<Decimal>,
    vol: Decimal,
    bid_vol: Decimal,
    ask_vol: Decimal,
}

impl TicksToTickBarsConsolidator {
    pub fn new(ticks_per_bar: u32, out_symbol: String, instrument: Instrument) -> Self {
        Self {
            ticks_per_bar,
            out_symbol,
            instrument,
            count: 0,
            start_ts: None,
            o: None,
            h: None,
            l: None,
            c: None,
            vol: Decimal::ZERO,
            bid_vol: Decimal::ZERO,
            ask_vol: Decimal::ZERO,
        }
    }

    pub fn update_tick(&mut self, tk: &Tick) -> Option<TickBar> {
        if self.start_ts.is_none() {
            self.start_ts = Some(tk.time);
        }
        if self.o.is_none() {
            self.o = Some(tk.price);
        }
        self.h = Some(self.h.map_or(tk.price, |x| x.max(tk.price)));
        self.l = Some(self.l.map_or(tk.price, |x| x.min(tk.price)));
        self.c = Some(tk.price);

        self.vol += tk.volume;
        match tk.side {
            TradeSide::Buy => self.ask_vol += tk.volume,
            TradeSide::Sell => self.bid_vol += tk.volume,
            TradeSide::None => {}
        }
        self.count += 1;

        if self.count >= self.ticks_per_bar
            && let (Some(start), Some(oo), Some(hh), Some(ll), Some(cc)) =
                (self.start_ts, self.o, self.h, self.l, self.c)
        {
            let end_incl = tk.time - Duration::nanoseconds(1);
            let out = TickBar {
                symbol: self.out_symbol.clone(),
                instrument: self.instrument.clone(),
                time_start: start,
                time_end: end_incl,
                open: oo,
                high: hh,
                low: ll,
                close: cc,
                volume: self.vol,
                ask_volume: self.ask_vol,
                bid_volume: self.bid_vol,
            };
            // reset state
            self.count = 0;
            self.start_ts = None;
            self.o = None;
            self.h = None;
            self.l = None;
            self.c = None;
            self.vol = Decimal::ZERO;
            self.bid_vol = Decimal::ZERO;
            self.ask_vol = Decimal::ZERO;
            return Some(out);
        }
        None
    }
}

pub struct TicksToCandlesConsolidator {
    dst: Resolution,
    out_symbol: String,
    hours: Option<Arc<MarketHours>>,
    instrument: Instrument,
    // state
    o: Option<Decimal>,
    h: Option<Decimal>,
    l: Option<Decimal>,
    c: Option<Decimal>,
    vol: Decimal,
    bid_vol: Decimal,
    ask_vol: Decimal,
    win: Option<Win>,
}

enum Win {
    Fixed {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        len: Duration,
    },
    Session {
        open: DateTime<Utc>,
        close: DateTime<Utc>,
    },
}

impl TicksToCandlesConsolidator {
    pub fn new(
        dst: Resolution,
        out_symbol: String,
        hours: Option<Arc<MarketHours>>,
        instrument: Instrument,
    ) -> Self {
        Self {
            dst,
            out_symbol,
            hours,
            instrument,
            o: None,
            h: None,
            l: None,
            c: None,
            vol: Decimal::ZERO,
            bid_vol: Decimal::ZERO,
            ask_vol: Decimal::ZERO,
            win: None,
        }
    }

    fn init_win(&self, t: DateTime<Utc>) -> Win {
        match self.dst {
            Resolution::Daily | Resolution::Weekly => {
                let mh = self
                    .hours
                    .as_ref()
                    .expect("Daily/Weekly requires MarketHours");
                let (open, close) = session_bounds(mh, t);
                Win::Session { open, close }
            }
            _ => {
                let len = fixed_len(&self.dst);
                let (start, end) = (floor_to(&self.dst, t), floor_to(&self.dst, t) + len);
                Win::Fixed { start, end, len }
            }
        }
    }

    fn inside(t: DateTime<Utc>, w: &Win) -> bool {
        match w {
            Win::Fixed { end, .. } => t < *end,
            Win::Session { open, close } => t >= *open && t < *close,
        }
    }

    fn advance(&self, t_last: DateTime<Utc>, w: &Win) -> Win {
        match w {
            Win::Fixed { end, len, .. } => {
                let mut s = *end;
                while t_last >= s {
                    s += *len;
                }
                Win::Fixed {
                    start: s - *len,
                    end: s,
                    len: *len,
                }
            }
            Win::Session { close, .. } => {
                let mh = self
                    .hours
                    .as_ref()
                    .expect("session advance needs MarketHours");
                let (nopen, nclose) = next_session_after(mh, *close);
                Win::Session {
                    open: nopen,
                    close: nclose,
                }
            }
        }
    }

    fn flush(&mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Option<Candle> {
        if let (Some(oo), Some(hh), Some(ll), Some(cc)) = (self.o, self.h, self.l, self.c) {
            let out = Candle {
                symbol: self.out_symbol.clone(),
                instrument: self.instrument.clone(),
                time_start: start,
                time_end: end_inclusive(end),
                open: oo,
                high: hh,
                low: ll,
                close: cc,
                volume: self.vol,
                ask_volume: self.ask_vol,
                bid_volume: self.bid_vol,
                resolution: self.dst,
            };
            // reset
            self.o = None;
            self.h = None;
            self.l = None;
            self.c = None;
            self.vol = Decimal::ZERO;
            self.bid_vol = Decimal::ZERO;
            self.ask_vol = Decimal::ZERO;
            return Some(out);
        }
        None
    }

    pub fn update_tick(&mut self, tk: &Tick) -> Option<Candle> {
        let t = tk.time;
        if self.win.is_none() {
            self.win = Some(self.init_win(t));
        }
        // drain windows until current tick is inside
        while !Self::inside(t, self.win.as_ref().unwrap()) {
            let (start, end) = match self.win.as_ref().unwrap() {
                Win::Fixed { start, end, .. } => (*start, *end),
                Win::Session { open, close } => (*open, *close),
            };
            let flushed = self.flush(start, end);
            let cur = self.win.take().unwrap();
            self.win = Some(self.advance(t, &cur));
            if flushed.is_some() {
                return flushed;
            }
        }
        // accumulate current tick
        if self.o.is_none() {
            self.o = Some(tk.price);
        }
        self.h = Some(self.h.map_or(tk.price, |x| x.max(tk.price)));
        self.l = Some(self.l.map_or(tk.price, |x| x.min(tk.price)));
        self.c = Some(tk.price);
        self.vol += tk.volume;
        match tk.side {
            TradeSide::Buy => self.ask_vol += tk.volume,
            TradeSide::Sell => self.bid_vol += tk.volume,
            TradeSide::None => {}
        }
        None
    }

    pub fn update_time(&mut self, t_now: DateTime<Utc>) -> Option<Candle> {
        if let Some(w) = self.win.as_ref() {
            let end = match w {
                Win::Fixed { end, .. } => *end,
                Win::Session { close, .. } => *close,
            };
            if t_now >= end {
                let (start, _end) = match w {
                    Win::Fixed { start, end, .. } => (*start, *end),
                    Win::Session { open, close } => (*open, *close),
                };
                let flushed = self.flush(start, end);
                // advance until t_now fits next window
                let mut next_w = self.win.take().unwrap();
                loop {
                    let next = self.advance(t_now, &next_w);
                    let end_next = match &next {
                        Win::Fixed { end, .. } => *end,
                        Win::Session { close, .. } => *close,
                    };
                    if t_now < end_next {
                        next_w = next;
                        break;
                    } else {
                        next_w = next;
                    }
                }
                self.win = Some(next_w);
                return flushed;
            }
        }
        None
    }
}

pub struct BboToCandlesConsolidator {
    dst: Resolution,
    out_symbol: String,
    hours: Option<Arc<MarketHours>>,
    instrument: Instrument,
    // state
    o: Option<Decimal>,
    h: Option<Decimal>,
    l: Option<Decimal>,
    c: Option<Decimal>,
    v: Decimal,
    win: Option<Win>,
}

impl BboToCandlesConsolidator {
    pub fn new(
        dst: Resolution,
        out_symbol: String,
        hours: Option<Arc<MarketHours>>,
        instrument: Instrument,
    ) -> Self {
        Self {
            dst,
            out_symbol,
            hours,
            instrument,
            o: None,
            h: None,
            l: None,
            c: None,
            v: Decimal::ZERO,
            win: None,
        }
    }

    fn init_win(&self, t: DateTime<Utc>) -> Win {
        TicksToCandlesConsolidator {
            dst: self.dst,
            out_symbol: String::new(),
            hours: self.hours.clone(),
            instrument: self.instrument.clone(),
            o: None,
            h: None,
            l: None,
            c: None,
            vol: Decimal::ZERO,
            bid_vol: Decimal::ZERO,
            ask_vol: Decimal::ZERO,
            win: None,
        }
        .init_win(t)
    }
    fn inside(t: DateTime<Utc>, w: &Win) -> bool {
        TicksToCandlesConsolidator::inside(t, w)
    }
    fn advance(&self, t_last: DateTime<Utc>, w: &Win) -> Win {
        TicksToCandlesConsolidator {
            dst: self.dst,
            out_symbol: String::new(),
            hours: self.hours.clone(),
            instrument: self.instrument.clone(),
            o: None,
            h: None,
            l: None,
            c: None,
            vol: Decimal::ZERO,
            bid_vol: Decimal::ZERO,
            ask_vol: Decimal::ZERO,
            win: None,
        }
        .advance(t_last, w)
    }

    fn flush(&mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Option<Candle> {
        if let (Some(oo), Some(hh), Some(ll), Some(cc)) = (self.o, self.h, self.l, self.c) {
            let out = Candle {
                symbol: self.out_symbol.clone(),
                instrument: self.instrument.clone(),
                time_start: start,
                time_end: end_inclusive(end),
                open: oo,
                high: hh,
                low: ll,
                close: cc,
                volume: self.v,
                ask_volume: dec!(0),
                bid_volume: dec!(0),
                resolution: self.dst,
            };
            self.o = None;
            self.h = None;
            self.l = None;
            self.c = None;
            self.v = Decimal::ZERO;
            return Some(out);
        }
        None
    }

    pub fn update_bbo(&mut self, bbo: &Bbo) -> Option<Candle> {
        let t = bbo.time;
        if self.win.is_none() {
            self.win = Some(self.init_win(t));
        }
        while !Self::inside(t, self.win.as_ref().unwrap()) {
            let (start, end) = match self.win.as_ref().unwrap() {
                Win::Fixed { start, end, .. } => (*start, *end),
                Win::Session { open, close } => (*open, *close),
            };
            let flushed = self.flush(start, end);
            let cur = self.win.take().unwrap();
            self.win = Some(self.advance(t, &cur));
            if flushed.is_some() {
                return flushed;
            }
        }
        let mid = (bbo.bid + bbo.ask) / Decimal::from(2);
        if self.o.is_none() {
            self.o = Some(mid);
        }
        self.h = Some(self.h.map_or(mid, |x| x.max(mid)));
        self.l = Some(self.l.map_or(mid, |x| x.min(mid)));
        self.c = Some(mid);
        None
    }

    pub fn update_time(&mut self, t_now: DateTime<Utc>) -> Option<Candle> {
        if let Some(w) = self.win.as_ref() {
            let end = match w {
                Win::Fixed { end, .. } => *end,
                Win::Session { close, .. } => *close,
            };
            if t_now >= end {
                let (start, _end) = match w {
                    Win::Fixed { start, end, .. } => (*start, *end),
                    Win::Session { open, close } => (*open, *close),
                };
                let flushed = self.flush(start, end);
                let mut next_w = self.win.take().unwrap();
                loop {
                    let next = self.advance(t_now, &next_w);
                    let end_next = match &next {
                        Win::Fixed { end, .. } => *end,
                        Win::Session { close, .. } => *close,
                    };
                    if t_now < end_next {
                        next_w = next;
                        break;
                    } else {
                        next_w = next;
                    }
                }
                self.win = Some(next_w);
                return flushed;
            }
        }
        None
    }
}

pub struct CandlesToCandlesConsolidator {
    dst: Resolution,
    out_symbol: String,
    hours: Option<Arc<MarketHours>>,
    instrument: Instrument,
    // state
    b_start: Option<DateTime<Utc>>,
    b_end: Option<DateTime<Utc>>,
    o: Option<Decimal>,
    h: Option<Decimal>,
    l: Option<Decimal>,
    c: Option<Decimal>,
    vol: Decimal,
    bid_vol: Decimal,
    ask_vol: Decimal,
    win: Option<Win>,
}

impl CandlesToCandlesConsolidator {
    pub fn new(
        dst: Resolution,
        out_symbol: String,
        hours: Option<Arc<MarketHours>>,
        instrument: Instrument,
    ) -> Self {
        Self {
            dst,
            out_symbol,
            hours,
            instrument,
            b_start: None,
            b_end: None,
            o: None,
            h: None,
            l: None,
            c: None,
            vol: Decimal::ZERO,
            bid_vol: Decimal::ZERO,
            ask_vol: Decimal::ZERO,
            win: None,
        }
    }

    fn init_win(&self, t: DateTime<Utc>) -> Win {
        match self.dst {
            Resolution::Daily | Resolution::Weekly => {
                let mh = self
                    .hours
                    .as_ref()
                    .expect("Daily/Weekly requires MarketHours");
                let (open, close) = session_bounds(mh, t);
                Win::Session { open, close }
            }
            _ => {
                let len = fixed_len(&self.dst);
                let start = floor_to(&self.dst, t);
                Win::Fixed {
                    start,
                    end: start + len,
                    len,
                }
            }
        }
    }

    fn contains(ts: DateTime<Utc>, w: &Win) -> bool {
        match w {
            Win::Fixed { start, end, .. } => ts >= *start && ts < *end,
            Win::Session { open, close } => ts >= *open && ts < *close,
        }
    }

    fn advance(&self, t_last: DateTime<Utc>, w: &Win) -> Win {
        match w {
            Win::Fixed { end, len, .. } => {
                let mut s = *end;
                while t_last >= s {
                    s += *len;
                }
                Win::Fixed {
                    start: s - *len,
                    end: s,
                    len: *len,
                }
            }
            Win::Session { close, .. } => {
                let mh = self
                    .hours
                    .as_ref()
                    .expect("session advance needs MarketHours");
                let (nopen, nclose) = next_session_after(mh, *close);
                Win::Session {
                    open: nopen,
                    close: nclose,
                }
            }
        }
    }

    fn flush(&mut self, _start: DateTime<Utc>, _end: DateTime<Utc>) -> Option<Candle> {
        if let (Some(bs), Some(be), Some(oo), Some(hh), Some(ll), Some(cc)) =
            (self.b_start, self.b_end, self.o, self.h, self.l, self.c)
        {
            let out = Candle {
                symbol: self.out_symbol.clone(),
                instrument: self.instrument.clone(),
                time_start: bs,
                time_end: end_inclusive(be),
                open: oo,
                high: hh,
                low: ll,
                close: cc,
                volume: self.vol,
                ask_volume: self.ask_vol,
                bid_volume: self.bid_vol,
                resolution: self.dst,
            };
            self.b_start = None;
            self.b_end = None;
            self.o = None;
            self.h = None;
            self.l = None;
            self.c = None;
            self.vol = Decimal::ZERO;
            self.ask_vol = Decimal::ZERO;
            self.bid_vol = Decimal::ZERO;
            return Some(out);
        }
        None
    }

    pub fn update_candle(&mut self, bar: &Candle) -> Option<Candle> {
        if bar.resolution == self.dst {
            return None;
        }
        let ts = bar.time_start;
        if self.win.is_none() {
            self.win = Some(self.init_win(ts));
        }
        while !Self::contains(ts, self.win.as_ref().unwrap()) {
            let (start, end) = match self.win.as_ref().unwrap() {
                Win::Fixed { start, end, .. } => (*start, *end),
                Win::Session { open, close } => (*open, *close),
            };
            let flushed = self.flush(start, end);
            let cur = self.win.take().unwrap();
            self.win = Some(self.advance(ts, &cur));
            if flushed.is_some() {
                return flushed;
            }
        }
        let (w_start, w_end) = match self.win.as_ref().unwrap() {
            Win::Fixed { start, end, .. } => (*start, *end),
            Win::Session { open, close } => (*open, *close),
        };
        if self.b_start.is_none() {
            self.b_start = Some(w_start);
        }
        self.b_end = Some(w_end.max(bar.time_end));
        if self.o.is_none() {
            self.o = Some(bar.open);
        }
        self.h = Some(self.h.map_or(bar.high, |x| x.max(bar.high)));
        self.l = Some(self.l.map_or(bar.low, |x| x.min(bar.low)));
        self.c = Some(bar.close);
        self.vol += bar.volume;
        self.ask_vol += bar.ask_volume;
        self.bid_vol += bar.bid_volume;
        None
    }
}

// === Trait implementations for engine-unified handling ===
impl Consolidator for TicksToTickBarsConsolidator {
    fn on_tick(&mut self, tk: &Tick) -> Option<ConsolidatedOut> {
        self.update_tick(tk).map(ConsolidatedOut::TickBar)
    }
}

impl Consolidator for TicksToCandlesConsolidator {
    fn on_tick(&mut self, tk: &Tick) -> Option<ConsolidatedOut> {
        self.update_tick(tk).map(ConsolidatedOut::Candle)
    }
    fn on_time(&mut self, t: DateTime<Utc>) -> Option<ConsolidatedOut> {
        self.update_time(t).map(ConsolidatedOut::Candle)
    }
}

impl Consolidator for BboToCandlesConsolidator {
    fn on_bbo(&mut self, b: &Bbo) -> Option<ConsolidatedOut> {
        self.update_bbo(b).map(ConsolidatedOut::Candle)
    }
    fn on_time(&mut self, t: DateTime<Utc>) -> Option<ConsolidatedOut> {
        self.update_time(t).map(ConsolidatedOut::Candle)
    }
}

impl Consolidator for CandlesToCandlesConsolidator {
    fn on_candle(&mut self, c: &Candle) -> Option<ConsolidatedOut> {
        self.update_candle(c).map(ConsolidatedOut::Candle)
    }
}
