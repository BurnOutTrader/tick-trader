use super::events::Side;
use crate::rkyv_types::DecimalDef;
use crate::securities::symbols::Instrument;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
+use ahash::AHashMap;

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
pub struct Lot {
    pub qty: i64,
    #[rkyv(with = DecimalDef)]
    pub price: Decimal,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
pub struct PositionSegment {
    pub id: u64,
    pub side: Side,
    pub open_qty: i64,
    pub net_qty: i64,
    #[rkyv(with = DecimalDef)]
    pub open_vwap_px: Decimal,
    #[rkyv(with = DecimalDef)]
    pub realized_pnl: Decimal,
    #[rkyv(with = DecimalDef)]
    pub fees: Decimal,
    pub lots_open: Vec<Lot>,
}

impl PositionSegment {
    pub fn new(id: u64, side: Side, qty: i64, px: Decimal) -> Self {
        let signed = qty * side.sign() as i64;
        Self {
            id,
            side,
            open_qty: qty,
            net_qty: signed,
            open_vwap_px: px,
            realized_pnl: Decimal::ZERO,
            fees: Decimal::ZERO,
            lots_open: vec![Lot { qty, price: px }],
        }
    }
}

#[derive(Default)]
pub struct PositionLedger {
    pub segments: AHashMap<Instrument, PositionSegment>,
    next_id: u64,
}

impl PositionLedger {
    pub fn new() -> Self {
        Self {
            segments: AHashMap::new(),
            next_id: 1,
        }
    }

    pub fn apply_fill(
        &mut self,
        instrument: &Instrument,
        side: Side,
        qty: i64,
        price: Decimal,
        fee: Decimal,
    ) -> (i64, Decimal) {
        let signed = (qty) * side.sign() as i64;
        let entry = self.segments.remove(instrument);
        match entry {
            None => {
                // Start a new segment
                let mut seg = PositionSegment::new(self.alloc_id(), side, qty, price);
                seg.fees += fee;
                self.segments.insert(instrument.clone(), seg);
                (qty * side.sign() as i64, Decimal::ZERO)
            }
            Some(mut seg) => {
                if seg.net_qty == 0 {
                    // closed somehow, start new
                    let mut seg2 = PositionSegment::new(self.alloc_id(), side, qty, price);
                    seg2.fees += fee;
                    self.segments.insert(instrument.clone(), seg2);
                    return (qty * side.sign() as i64, Decimal::ZERO);
                }
                let seg_sign = match seg.side {
                    Side::Buy => 1,
                    Side::Sell => -1,
                } as i64;
                if seg_sign == side.sign() as i64 {
                    // same side: accumulate; update VWAP
                    let old_abs = seg.net_qty.abs();
                    let new_abs = old_abs + qty.abs();
                    if new_abs > 0 {
                        seg.open_vwap_px = ((seg.open_vwap_px
                            * Decimal::from_i64(old_abs).unwrap())
                            + (price * Decimal::from_i64(qty.abs()).unwrap()))
                            / Decimal::from_i64(new_abs).unwrap();
                    }
                    seg.net_qty += signed;
                    seg.lots_open.push(Lot { qty, price });
                    seg.fees += fee;
                    let net_after = seg.net_qty * seg_sign;
                    self.segments.insert(instrument.clone(), seg);
                    (net_after, Decimal::ZERO)
                } else {
                    // opposite side: close against existing up to min
                    let close_qty = i64::min(qty.abs(), seg.net_qty.abs());
                    let realized = Decimal::from_i64(close_qty).unwrap()
                        * (price - seg.open_vwap_px)
                        * Decimal::from_i32(seg_sign as i32).unwrap();
                    seg.realized_pnl += realized;
                    seg.net_qty -= close_qty * seg_sign; // reduce towards zero
                    seg.fees += fee;
                    let remainder = qty.abs() - close_qty;
                    let realized_total = realized;
                    let mut net_after = seg.net_qty * seg_sign; // signed by old side
                    if seg.net_qty == 0 {
                        // segment closes; if an overshoot, start new with a remainder on new side
                        if remainder > 0 {
                            let seg2 =
                                PositionSegment::new(self.alloc_id(), side, remainder, price);
                            self.segments.insert(instrument.clone(), seg2);
                            net_after = remainder * side.sign() as i64;
                        } else {
                            // no remainder; nothing kept
                        }
                    } else {
                        // still the same segment active
                        self.segments.insert(instrument.clone(), seg);
                    }
                    (net_after, realized_total)
                }
            }
        }
    }

    #[inline]
    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::securities::symbols::Instrument;
    use rust_decimal::prelude::FromPrimitive;

    #[test]
    fn accumulate_and_cross_flat() {
        let inst = Instrument::try_from("ESZ5").unwrap();
        let mut l = PositionLedger::new();
        // buy 2 @ 100
        l.apply_fill(
            &inst,
            Side::Buy,
            2,
            Decimal::from_i32(100).unwrap(),
            Decimal::ZERO,
        );
        // buy 1 @ 110 (vwap=103.33)
        l.apply_fill(
            &inst,
            Side::Buy,
            1,
            Decimal::from_i32(110).unwrap(),
            Decimal::ZERO,
        );
        // sell 4 @ 120 -> close 3 at 103.33, realized 16.67*3 = 50.01, remainder opens 1 short @120
        let (net_after, realized) = l.apply_fill(
            &inst,
            Side::Sell,
            4,
            Decimal::from_i32(120).unwrap(),
            Decimal::ZERO,
        );
        assert_eq!(net_after, -1);
        assert!(realized > Decimal::from_f32(49.9).unwrap());
    }

    #[test]
    fn same_side_accumulates_and_updates_vwap() {
        let inst = Instrument::try_from("CLZ5").unwrap();
        let mut l = PositionLedger::new();
        l.apply_fill(
            &inst,
            Side::Buy,
            1,
            Decimal::from_i32(100).unwrap(),
            Decimal::ZERO,
        );
        l.apply_fill(
            &inst,
            Side::Buy,
            3,
            Decimal::from_i32(110).unwrap(),
            Decimal::ZERO,
        );
        let seg = l.segments.get(&inst).unwrap();
        assert_eq!(seg.net_qty, 4);
        let expected_vwap = (Decimal::from_i32(100).unwrap() * Decimal::from_i32(1).unwrap()
            + Decimal::from_i32(110).unwrap() * Decimal::from_i32(3).unwrap())
            / Decimal::from_i32(4).unwrap();
        assert_eq!(seg.open_vwap_px, expected_vwap);
    }

    #[test]
    fn close_to_flat_exact() {
        let inst = Instrument::try_from("NQZ5").unwrap();
        let mut l = PositionLedger::new();
        l.apply_fill(
            &inst,
            Side::Sell,
            5,
            Decimal::from_i32(150).unwrap(),
            Decimal::ZERO,
        );
        let (net_after, realized) = l.apply_fill(
            &inst,
            Side::Buy,
            5,
            Decimal::from_i32(140).unwrap(),
            Decimal::ZERO,
        );
        assert_eq!(net_after, 0);
        // Short at 150, buy back at 140 => +10 * 5 = +50
        assert_eq!(realized, Decimal::from_i32(50).unwrap());
        assert_eq!(l.segments.get(&inst).map(|s| s.net_qty).unwrap_or(0), 0);
    }

    #[test]
    fn overshoot_reverses_and_starts_new_segment() {
        let inst = Instrument::try_from("ESZ5").unwrap();
        let mut l = PositionLedger::new();
        l.apply_fill(
            &inst,
            Side::Buy,
            2,
            Decimal::from_i32(100).unwrap(),
            Decimal::ZERO,
        );
        // sell 5 @ 90 => close 2, realized -10*2 = -20; remainder 3 short opens @ 90
        let (net_after, realized) = l.apply_fill(
            &inst,
            Side::Sell,
            5,
            Decimal::from_i32(90).unwrap(),
            Decimal::ZERO,
        );
        assert_eq!(net_after, -3);
        assert_eq!(realized, Decimal::from_i32(-20).unwrap());
        let seg = l.segments.get(&inst).unwrap();
        assert_eq!(seg.side, Side::Sell);
        assert_eq!(seg.net_qty, -3);
        assert_eq!(seg.open_vwap_px, Decimal::from_i32(90).unwrap());
    }

    #[test]
    fn fees_accumulate_on_segment() {
        let inst = Instrument::try_from("ESZ5").unwrap();
        let mut l = PositionLedger::new();
        l.apply_fill(
            &inst,
            Side::Buy,
            1,
            Decimal::from_i32(100).unwrap(),
            Decimal::from_f32(1.25).unwrap(),
        );
        l.apply_fill(
            &inst,
            Side::Buy,
            1,
            Decimal::from_i32(101).unwrap(),
            Decimal::from_f32(0.75).unwrap(),
        );
        let seg = l.segments.get(&inst).unwrap();
        assert_eq!(seg.fees, Decimal::from_f32(2.0).unwrap());
    }
}
