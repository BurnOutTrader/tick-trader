use projectx::http::models::{BracketCfg, PlaceOrderReq};

#[test]
fn place_order_full_serializes_camel_case() {
    let req = PlaceOrderReq {
        account_id: 465,
        contract_id: "CON.F.US.DA6.M25".to_string(),
        type_: 2,
        side: 1,
        size: 1,
        limit_price: None,
        stop_price: None,
        trail_price: None,
        custom_tag: None,
        stop_loss_bracket: Some(BracketCfg {
            ticks: 10,
            type_: 1,
        }),
        take_profit_bracket: Some(BracketCfg {
            ticks: 20,
            type_: 1,
        }),
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"contractId\""));
    assert!(json.contains("\"type\""));
    assert!(json.contains("\"side\""));
    assert!(json.contains("\"size\""));
    assert!(json.contains("\"stopLossBracket\""));
    assert!(json.contains("\"takeProfitBracket\""));
    assert!(json.contains("\"ticks\""));
}

#[test]
fn place_order_min_omits_optionals() {
    let req = PlaceOrderReq {
        account_id: 1,
        contract_id: "CID".to_string(),
        type_: 2,
        side: 0,
        size: 3,
        limit_price: None,
        stop_price: None,
        trail_price: None,
        custom_tag: None,
        stop_loss_bracket: None,
        take_profit_bracket: None,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"contractId\""));
    assert!(json.contains("\"type\""));
    assert!(!json.contains("limitPrice"));
    assert!(!json.contains("stopPrice"));
    assert!(!json.contains("trailPrice"));
    assert!(!json.contains("customTag"));
    assert!(!json.contains("stopLossBracket"));
    assert!(!json.contains("takeProfitBracket"));
}
