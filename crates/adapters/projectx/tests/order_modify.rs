use projectx::http::models::ModifyOrderReq;

#[test]
fn modify_order_req_serializes_camel_case() {
    let req = ModifyOrderReq {
        account_id: 465,
        order_id: 26974,
        size: Some(1),
        limit_price: None,
        stop_price: Some(1604.0),
        trail_price: None,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"orderId\""));
    assert!(json.contains("\"size\""));
    assert!(json.contains("\"stopPrice\""));
}

#[test]
fn modify_order_req_omits_optionals() {
    let req = ModifyOrderReq {
        account_id: 1,
        order_id: 2,
        size: None,
        limit_price: None,
        stop_price: None,
        trail_price: None,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"orderId\""));
    assert!(!json.contains("size"));
    assert!(!json.contains("limitPrice"));
    assert!(!json.contains("stopPrice"));
    assert!(!json.contains("trailPrice"));
}
