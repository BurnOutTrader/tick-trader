use projectx::http::models::CancelOrderReq;

#[test]
fn cancel_order_req_serializes_camel_case() {
    let req = CancelOrderReq {
        account_id: 465,
        order_id: 26974,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"orderId\""));
    assert!(json.contains("465"));
    assert!(json.contains("26974"));
}
