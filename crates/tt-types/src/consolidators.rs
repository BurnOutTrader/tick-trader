use crate::base_data::{Bbo, Candle, Feed, Resolution, Side, Tick, TickBar};
use crate::securities::market_hours::{MarketHours, next_session_after, session_bounds};
use crate::securities::symbols::{Exchange, Instrument};
use chrono::{DateTime, Duration, TimeZone, Utc};
use rust_decimal::{Decimal, dec};
use std::sync::Arc;
use tokio::sync::broadcast;

pub trait Consolidator<T>: Send + Sync {
    fn subscribe(&self, _feed: Feed) -> broadcast::Receiver<T>;
    fn kill(&self);
}

pub enum InboundRx {
    Ticks(broadcast::Receiver<Tick>),
    Bbo(broadcast::Receiver<Bbo>),
    Candles(broadcast::Receiver<Candle>),
}

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

// ===================== Consolidators =====================
//
// Each consolidator owns an outbound broadcast::Sender<Candle> and a task
// that consumes an inbound Receiver<...> and produces candles.
// subscribe() returns a Receiver<Candle>. kill() aborts the task.
//
// ---------------------------------------------------------
pub struct TicksToTickBarsConsolidator {
    out_tx: Arc<broadcast::Sender<TickBar>>,
    task: tokio::task::JoinHandle<()>,
}

#[allow(unused)]
impl TicksToTickBarsConsolidator {
    pub fn new(
        ticks_per_bar: u32,
        mut rx_tick: broadcast::Receiver<Tick>, // inbound
        out_symbol: String,
        out_exchange: Exchange,
        instrument: Instrument,
    ) -> Self {
        let (out_tx, _) = broadcast::channel::<TickBar>(1024);

        let out_tx = Arc::new(out_tx);
        let out_tx_clone = Arc::clone(&out_tx);

        let task = tokio::spawn(async move {
            let mut count: u32 = 0;
            let mut start_ts: Option<DateTime<Utc>> = None;

            let mut o: Option<Decimal> = None;
            let mut h: Option<Decimal> = None;
            let mut l: Option<Decimal> = None;
            let mut c: Option<Decimal> = None;

            let mut vol = Decimal::ZERO;
            let mut bid_vol = Decimal::ZERO;
            let mut ask_vol = Decimal::ZERO;
            let mut trades = Decimal::ZERO;

            while let Ok(tk) = rx_tick.recv().await {
                if start_ts.is_none() {
                    start_ts = Some(tk.time);
                }
                if o.is_none() {
                    o = Some(tk.price);
                }

                h = Some(h.map_or(tk.price, |x| x.max(tk.price)));
                l = Some(l.map_or(tk.price, |x| x.min(tk.price)));
                c = Some(tk.price);

                vol += tk.volume;
                trades += dec!(1);
                match tk.side {
                    Side::Buy => ask_vol += tk.volume,
                    Side::Sell => bid_vol += tk.volume,
                    Side::None => {}
                }
                count += 1;
                if count >= ticks_per_bar {
                    if let (Some(start), Some(oo), Some(hh), Some(ll), Some(cc)) =
                        (start_ts, o, h, l, c)
                    {
                        // Make the end time inclusive by subtracting 1ns if you follow that convention elsewhere
                        let end_inclusive = tk.time - Duration::nanoseconds(1);
                        let _ = out_tx.send(TickBar {
                            symbol: out_symbol.clone(),
                            instrument: instrument.clone(),
                            time_start: start,
                            time_end: end_inclusive,
                            open: oo,
                            high: hh,
                            low: ll,
                            close: cc,
                            volume: vol,
                            ask_volume: ask_vol,
                            bid_volume: bid_vol,
                        });
                    }

                    // reset for next bar
                    count = 0;
                    start_ts = None;
                    o = None;
                    h = None;
                    l = None;
                    c = None;
                    vol = Decimal::ZERO;
                    bid_vol = Decimal::ZERO;
                    ask_vol = Decimal::ZERO;
                    trades = Decimal::ZERO;
                }
            }
        });

        Self {
            out_tx: out_tx_clone,
            task,
        }
    }
}

impl Consolidator<TickBar> for TicksToTickBarsConsolidator {
    fn subscribe(&self, _feed: Feed) -> broadcast::Receiver<TickBar> {
        self.out_tx.subscribe()
    }
    fn kill(&self) {
        self.task.abort();
    }
}

pub struct TicksToCandlesConsolidator {
    out_tx: Arc<broadcast::Sender<Candle>>,
    task: tokio::task::JoinHandle<()>,
}

impl TicksToCandlesConsolidator {
    pub fn new(
        dst: Resolution,
        mut rx_tick: broadcast::Receiver<Tick>,
        out_symbol: String,
        hours: Option<Arc<MarketHours>>,
        instrument: Instrument,
        mut rx_time: Option<broadcast::Receiver<DateTime<Utc>>>,
    ) -> Self {
        let (out_tx, _) = broadcast::channel::<Candle>(1024);

        let out_tx = Arc::new(out_tx);
        let out_tx_clone = Arc::clone(&out_tx);

        let task = tokio::spawn(async move {
            let mut o = None;
            let mut h = None;
            let mut l = None;
            let mut c = None;
            let mut vol = Decimal::ZERO;
            let mut bid_vol = Decimal::ZERO;
            let mut ask_vol = Decimal::ZERO;
            let mut trades = Decimal::ZERO;

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
            let mut win: Option<Win> = None;

            let init_win =
                |t: DateTime<Utc>, res: &Resolution, hours: &Option<Arc<MarketHours>>| -> Win {
                    match res {
                        Resolution::Daily | Resolution::Weekly => {
                            let mh = hours.as_ref().expect("Daily/Weekly requires MarketHours");
                            let (open, close) = session_bounds(mh, t);
                            Win::Session { open, close }
                        }
                        _ => {
                            let len = fixed_len(res);
                            let (start, end) = (floor_to(res, t), floor_to(res, t) + len);
                            Win::Fixed { start, end, len }
                        }
                    }
                };

            let inside = |t: DateTime<Utc>, w: &Win| -> bool {
                match w {
                    Win::Fixed { end, .. } => t < *end,
                    Win::Session { open, close } => t >= *open && t < *close,
                }
            };

            let advance =
                |t_last: DateTime<Utc>, w: &Win, hours: &Option<Arc<MarketHours>>| -> Win {
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
                            let mh = hours.as_ref().expect("session advance needs MarketHours");
                            let (nopen, nclose) = next_session_after(mh, *close);
                            Win::Session {
                                open: nopen,
                                close: nclose,
                            }
                        }
                    }
                };

            loop {
                tokio::select! {
                    // Market data tick advances state and may flush if crossing into next window
                    Ok(tk) = rx_tick.recv() => {
                        let t = tk.time;
                        if win.is_none() {
                            win = Some(init_win(t, &dst, &hours));
                        }
                        while !inside(t, win.as_ref().unwrap()) {
                            if let (Some(oo), Some(hh), Some(ll), Some(cc)) = (o, h, l, c) {
                                let (start, end) = match win.as_ref().unwrap() {
                                    Win::Fixed { start, end, .. } => (*start, *end),
                                    Win::Session { open, close } => (*open, *close),
                                };
                                let _ = out_tx.send(Candle {
                                    symbol: out_symbol.clone(),
                                    instrument: instrument.clone(),
                                    time_start: start,
                                    time_end: end_inclusive(end),
                                    open: oo,
                                    high: hh,
                                    low: ll,
                                    close: cc,
                                    volume: vol,
                                    ask_volume: ask_vol,
                                    bid_volume: bid_vol,
                                    resolution: dst.clone(),
                                });
                            }
                            // reset & advance
                            o = None; h = None; l = None; c = None;
                            vol = Decimal::ZERO; bid_vol = Decimal::ZERO; ask_vol = Decimal::ZERO; trades = Decimal::ZERO;
                            let cur = win.take().unwrap();
                            win = Some(advance(t, &cur, &hours));
                        }
                        if o.is_none() { o = Some(tk.price); }
                        h = Some(h.map_or(tk.price, |x| x.max(tk.price)));
                        l = Some(l.map_or(tk.price, |x| x.min(tk.price)));
                        c = Some(tk.price);
                        vol += tk.volume; trades += dec!(1);
                        match tk.side { Side::Buy => ask_vol += tk.volume, Side::Sell => bid_vol += tk.volume, Side::None => {} }
                    }
                    // Time ticks: flush current window if we've reached/passed end, without requiring a new tick
                    Ok(t_now) = async {
                        if let Some(rx) = rx_time.as_mut() { rx.recv().await } else { Err(tokio::sync::broadcast::error::RecvError::Closed) }
                    }, if rx_time.is_some() => {
                        if let Some(w) = win.as_ref() {
                            let end = match w { Win::Fixed { end, .. } => *end, Win::Session { close, .. } => *close };
                            if t_now >= end {
                                if let (Some(oo), Some(hh), Some(ll), Some(cc)) = (o, h, l, c) {
                                    let (start, _end) = match w { Win::Fixed { start, end, .. } => (*start, *end), Win::Session { open, close } => (*open, *close) };
                                    let _ = out_tx.send(Candle {
                                        symbol: out_symbol.clone(),
                                        time_start: start,
                                        time_end: end_inclusive(end),
                                        open: oo,
                                        high: hh,
                                        low: ll,
                                        close: cc,
                                        volume: vol,
                                        ask_volume: ask_vol,
                                        bid_volume: bid_vol,
                                        instrument: instrument.clone(),
                                        resolution: dst.clone(),
                                    });
                                }
                                // reset & advance based on time
                                o = None; h = None; l = None; c = None;
                                vol = Decimal::ZERO; bid_vol = Decimal::ZERO; ask_vol = Decimal::ZERO; trades = Decimal::ZERO;
                                let cur = win.take().unwrap();
                                // advance until window end is after t_now
                                let mut next_w = cur;
                                loop {
                                    let next = advance(t_now, &next_w, &hours);
                                    let end_next = match &next { Win::Fixed { end, .. } => *end, Win::Session { close, .. } => *close };
                                    if t_now < end_next { next_w = next; break; } else { next_w = next; }
                                }
                                win = Some(next_w);
                            }
                        }
                    }
                    else => { break; }
                }
            }
        });

        Self {
            out_tx: out_tx_clone,
            task,
        }
    }
}

impl Consolidator<Candle> for TicksToCandlesConsolidator {
    fn subscribe(&self, _feed: Feed) -> broadcast::Receiver<Candle> {
        self.out_tx.subscribe()
    }
    fn kill(&self) {
        self.task.abort();
    }
}

pub struct BboToCandlesConsolidator {
    out_tx: Arc<broadcast::Sender<Candle>>,
    task: tokio::task::JoinHandle<()>,
}

impl BboToCandlesConsolidator {
    pub fn new(
        dst: Resolution,
        mut rx_bbo: broadcast::Receiver<Bbo>,
        out_symbol: String,
        hours: Option<Arc<MarketHours>>,
        instrument: Instrument,
        mut rx_time: Option<broadcast::Receiver<DateTime<Utc>>>,
    ) -> Self {
        let (out_tx, _) = broadcast::channel::<Candle>(1024);
        let out_tx = Arc::new(out_tx);
        let out_tx_clone = Arc::clone(&out_tx);

        let task = tokio::spawn(async move {
            let mut o = None;
            let mut h = None;
            let mut l = None;
            let mut c = None;
            let mut v = Decimal::ZERO;

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
            let mut win: Option<Win> = None;

            let init_win =
                |t: DateTime<Utc>, res: &Resolution, hours: &Option<Arc<MarketHours>>| -> Win {
                    match res {
                        Resolution::Daily | Resolution::Weekly => {
                            let mh = hours.as_ref().expect("Daily/Weekly requires MarketHours");
                            let (open, close) = session_bounds(mh, t);
                            Win::Session { open, close }
                        }
                        _ => {
                            let len = fixed_len(res);
                            let start = floor_to(res, t);
                            Win::Fixed {
                                start,
                                end: start + len,
                                len,
                            }
                        }
                    }
                };

            let inside = |t: DateTime<Utc>, w: &Win| -> bool {
                match w {
                    Win::Fixed { end, .. } => t < *end,
                    Win::Session { open, close } => t >= *open && t < *close,
                }
            };

            let advance =
                |t_last: DateTime<Utc>, w: &Win, hours: &Option<Arc<MarketHours>>| -> Win {
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
                            let mh = hours.as_ref().expect("session advance needs MarketHours");
                            let (nopen, nclose) = next_session_after(mh, *close);
                            Win::Session {
                                open: nopen,
                                close: nclose,
                            }
                        }
                    }
                };

            loop {
                tokio::select! {
                    Ok(bbo) = rx_bbo.recv() => {
                        let t = bbo.time;
                        if win.is_none() { win = Some(init_win(t, &dst, &hours)); }
                        while !inside(t, win.as_ref().unwrap()) {
                            if let (Some(oo), Some(hh), Some(ll), Some(cc)) = (o, h, l, c) {
                                let (start, end) = match win.as_ref().unwrap() {
                                    Win::Fixed { start, end, .. } => (*start, *end),
                                    Win::Session { open, close } => (*open, *close),
                                };
                                let _ = out_tx.send(Candle {
                                    symbol: out_symbol.clone(),
                                    time_start: start,
                                    time_end: end_inclusive(end),
                                    open: oo,
                                    high: hh,
                                    low: ll,
                                    close: cc,
                                    volume: v,
                                    ask_volume: dec!(0),
                                    bid_volume: dec!(0),
                                    instrument: instrument.clone(),
                                    resolution: dst.clone(),
                                });
                            }
                            o = None; h = None; l = None; c = None; v = Decimal::ZERO;
                            let cur = win.take().unwrap();
                            win = Some(advance(t, &cur, &hours));
                        }
                        let mid = (bbo.bid + bbo.ask) / Decimal::from(2);
                        if o.is_none() { o = Some(mid); }
                        h = Some(h.map_or(mid, |x| x.max(mid)));
                        l = Some(l.map_or(mid, |x| x.min(mid)));
                        c = Some(mid);
                    }
                    Ok(t_now) = async { if let Some(rx) = rx_time.as_mut() { rx.recv().await } else { Err(tokio::sync::broadcast::error::RecvError::Closed) } }, if rx_time.is_some() => {
                        if let Some(w) = win.as_ref() {
                            let end = match w { Win::Fixed { end, .. } => *end, Win::Session { close, .. } => *close };
                            if t_now >= end {
                                if let (Some(oo), Some(hh), Some(ll), Some(cc)) = (o, h, l, c) {
                                    let (start, _end) = match w { Win::Fixed { start, end, .. } => (*start, *end), Win::Session { open, close } => (*open, *close) };
                                    let _ = out_tx.send(Candle {
                                        symbol: out_symbol.clone(),
                                        time_start: start,
                                        time_end: end_inclusive(end),
                                        open: oo,
                                        high: hh,
                                        low: ll,
                                        close: cc,
                                        volume: v,
                                        ask_volume: dec!(0),
                                        bid_volume: dec!(0),
                                        instrument: instrument.clone(),
                                        resolution: dst.clone(),
                                    });
                                }
                                o = None; h = None; l = None; c = None; v = Decimal::ZERO;
                                let cur = win.take().unwrap();
                                // advance loop until t_now inside next
                                let mut next_w = cur;
                                loop {
                                    let next = advance(t_now, &next_w, &hours);
                                    let end_next = match &next { Win::Fixed { end, .. } => *end, Win::Session { close, .. } => *close };
                                    if t_now < end_next { next_w = next; break; } else { next_w = next; }
                                }
                                win = Some(next_w);
                            }
                        }
                    }
                    else => { break; }
                }
            }
        });

        Self {
            out_tx: out_tx_clone,
            task,
        }
    }
}

impl Consolidator<Candle> for BboToCandlesConsolidator {
    fn subscribe(&self, _feed: Feed) -> broadcast::Receiver<Candle> {
        self.out_tx.subscribe()
    }
    fn kill(&self) {
        self.task.abort();
    }
}

pub struct CandlesToCandlesConsolidator {
    out_tx: Arc<broadcast::Sender<Candle>>,
    task: tokio::task::JoinHandle<()>,
}

impl CandlesToCandlesConsolidator {
    pub fn new(
        dst: Resolution,
        mut rx_candle: broadcast::Receiver<Candle>,
        out_symbol: String,
        hours: Option<Arc<MarketHours>>,
        instrument: Instrument,
    ) -> Self {
        let (out_tx, _) = broadcast::channel::<Candle>(1024);
        let out_tx = Arc::new(out_tx);
        let out_tx_clone = Arc::clone(&out_tx);

        let task = tokio::spawn(async move {
            let mut b_start: Option<DateTime<Utc>> = None;
            let mut b_end: Option<DateTime<Utc>> = None;
            let mut o = None;
            let mut h = None;
            let mut l = None;
            let mut c = None;
            let mut vol = Decimal::ZERO;
            let mut bid_vol = Decimal::ZERO;
            let mut ask_vol = Decimal::ZERO;

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
            let mut win: Option<Win> = None;

            let init_win =
                |t: DateTime<Utc>, res: &Resolution, hours: &Option<Arc<MarketHours>>| -> Win {
                    match res {
                        Resolution::Daily | Resolution::Weekly => {
                            let mh = hours.as_ref().expect("Daily/Weekly requires MarketHours");
                            let (open, close) = session_bounds(mh, t);
                            Win::Session { open, close }
                        }
                        _ => {
                            let len = fixed_len(res);
                            let start = floor_to(res, t);
                            Win::Fixed {
                                start,
                                end: start + len,
                                len,
                            }
                        }
                    }
                };

            let contains = |ts: DateTime<Utc>, w: &Win| -> bool {
                match w {
                    Win::Fixed { start, end, .. } => ts >= *start && ts < *end,
                    Win::Session { open, close } => ts >= *open && ts < *close,
                }
            };

            let advance =
                |t_last: DateTime<Utc>, w: &Win, hours: &Option<Arc<MarketHours>>| -> Win {
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
                            let mh = hours.as_ref().expect("session advance needs MarketHours");
                            let (nopen, nclose) = next_session_after(mh, *close);
                            Win::Session {
                                open: nopen,
                                close: nclose,
                            }
                        }
                    }
                };

            while let Ok(bar) = rx_candle.recv().await {
                if bar.resolution == dst {
                    continue;
                }

                let ts = bar.time_start;
                if win.is_none() {
                    win = Some(init_win(ts, &dst, &hours));
                }

                while !contains(ts, win.as_ref().unwrap()) {
                    if let (Some(bs), Some(be), Some(oo), Some(hh), Some(ll), Some(cc)) =
                        (b_start, b_end, o, h, l, c)
                    {
                        let _ = out_tx.send(Candle {
                            symbol: out_symbol.clone(),
                            time_start: bs,
                            time_end: end_inclusive(be),
                            open: oo,
                            high: hh,
                            low: ll,
                            close: cc,
                            volume: vol,
                            ask_volume: ask_vol,
                            bid_volume: bid_vol,
                            instrument: instrument.clone(),
                            resolution: dst.clone(),
                        });
                    }
                    b_start = None;
                    b_end = None;
                    o = None;
                    h = None;
                    l = None;
                    c = None;
                    vol = Decimal::ZERO;
                    ask_vol = Decimal::ZERO;
                    bid_vol = Decimal::ZERO;
                    let cur = win.take().unwrap();
                    win = Some(advance(ts, &cur, &hours));
                }

                let (w_start, w_end) = match win.as_ref().unwrap() {
                    Win::Fixed { start, end, .. } => (*start, *end),
                    Win::Session { open, close } => (*open, *close),
                };
                if b_start.is_none() {
                    b_start = Some(w_start);
                }
                b_end = Some(w_end.max(bar.time_end));

                if o.is_none() {
                    o = Some(bar.open);
                }
                h = Some(h.map_or(bar.high, |x| x.max(bar.high)));
                l = Some(l.map_or(bar.low, |x| x.min(bar.low)));
                c = Some(bar.close);

                vol += bar.volume;
                ask_vol += bar.ask_volume;
                bid_vol += bar.bid_volume;
            }

            if let (Some(bs), Some(be), Some(oo), Some(hh), Some(ll), Some(cc)) =
                (b_start, b_end, o, h, l, c)
            {
                let _ = out_tx.send(Candle {
                    symbol: out_symbol,
                    instrument: instrument.clone(),
                    time_start: bs,
                    time_end: end_inclusive(be),
                    open: oo,
                    high: hh,
                    low: ll,
                    close: cc,
                    volume: vol,
                    ask_volume: ask_vol,
                    bid_volume: bid_vol,
                    resolution: dst,
                });
            }
        });

        Self {
            out_tx: out_tx_clone,
            task,
        }
    }
}

impl Consolidator<Candle> for CandlesToCandlesConsolidator {
    fn subscribe(&self, _feed: Feed) -> broadcast::Receiver<Candle> {
        self.out_tx.subscribe()
    }
    fn kill(&self) {
        self.task.abort();
    }
}
