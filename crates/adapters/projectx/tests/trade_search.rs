use projectx::http::models::TradeSearchReq;

#[test]
fn trade_search_req_serializes_camel_case_with_end() {
    let req = TradeSearchReq {
        account_id: 203,
        start_timestamp: "2025-01-20T15:47:39.882Z".to_string(),
        end_timestamp: Some("2025-01-30T15:47:39.882Z".to_string()),
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"startTimestamp\""));
    assert!(json.contains("\"endTimestamp\""));
}

#[test]
fn trade_search_req_omits_end_when_none() {
    let req = TradeSearchReq {
        account_id: 203,
        start_timestamp: "2025-01-20T15:47:39.882Z".to_string(),
        end_timestamp: None,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"startTimestamp\""));
    assert!(!json.contains("endTimestamp"));
}
