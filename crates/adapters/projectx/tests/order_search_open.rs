use projectx::http::models::OrderSearchOpenReq;

#[test]
fn order_search_open_req_serializes_camel_case() {
    let req = OrderSearchOpenReq { account_id: 212 };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("212"));
}
