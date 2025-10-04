use projectx::http::models::OrderSearchReq;

#[test]
fn order_search_req_serializes_camel_case_with_end() {
    let req = OrderSearchReq {
        account_id: 704,
        start_timestamp: "2025-07-18T21:00:01.268009+00:00".to_string(),
        end_timestamp: Some("2025-07-18T21:00:01.278009+00:00".to_string()),
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"startTimestamp\""));
    assert!(json.contains("\"endTimestamp\""));
}

#[test]
fn order_search_req_omits_end_when_none() {
    let req = OrderSearchReq {
        account_id: 704,
        start_timestamp: "2025-07-18T21:00:01.268009+00:00".to_string(),
        end_timestamp: None,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"startTimestamp\""));
    assert!(!json.contains("endTimestamp"));
}
