use projectx::http::models::PositionSearchOpenReq;

#[test]
fn position_search_open_req_serializes_camel_case() {
    let req = PositionSearchOpenReq { account_id: 536 };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("536"));
}
