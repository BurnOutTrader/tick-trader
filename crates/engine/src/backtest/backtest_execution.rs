use crate::backtest::realism_models::project_x::fill::FillConfig;
use crate::backtest::realism_models::traits::{
    FeeModel, FillModel, LatencyModel, RiskModel, SessionCalendar, SlippageModel,
};
/// Placeholder provider type; expanded in future iterations.
pub struct FauxExecutionProvider<
    M: FillModel,
    S: SlippageModel,
    L: LatencyModel,
    F: FeeModel,
    C: SessionCalendar,
    R: RiskModel,
> {
    #[allow(dead_code)]
    cfg: FillConfig,
    #[allow(dead_code)]
    fill: M,
    #[allow(dead_code)]
    slip: S,
    #[allow(dead_code)]
    lat: L,
    #[allow(dead_code)]
    fees: F,
    #[allow(dead_code)]
    cal: C,
    #[allow(dead_code)]
    risk: R,
    #[allow(dead_code)]
    seed: u64,
}

impl<M, S, L, F, C, R> FauxExecutionProvider<M, S, L, F, C, R>
where
    M: FillModel,
    S: SlippageModel,
    L: LatencyModel,
    F: FeeModel,
    C: SessionCalendar,
    R: RiskModel,
{
    #[allow(clippy::too_many_arguments, dead_code)]
    pub fn new(
        cfg: FillConfig,
        fill: M,
        slip: S,
        lat: L,
        fees: F,
        cal: C,
        risk: R,
        seed: u64,
    ) -> Self {
        Self {
            cfg,
            fill,
            slip,
            lat,
            fees,
            cal,
            risk,
            seed,
        }
    }
}
