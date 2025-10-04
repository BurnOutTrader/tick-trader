use projectx::http::models::ContractSearchReq;

#[test]
fn contract_search_req_serializes_camel_case() {
    let req = ContractSearchReq {
        live: false,
        search_text: "NQ".to_string(),
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"searchText\""));
    assert!(json.contains("\"live\""));
    assert!(json.contains("NQ"));
}
