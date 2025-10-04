use projectx::http::models::RetrieveBarsReq;

#[test]
fn retrieve_bars_req_serializes_camel_case() {
    let req = RetrieveBarsReq {
        contract_id: "CON.F.US.RTY.Z24".to_string(),
        live: false,
        start_time: "2024-12-01T00:00:00Z".to_string(),
        end_time: "2024-12-31T21:00:00Z".to_string(),
        unit: 3,
        unit_number: 1,
        limit: 7,
        include_partial_bar: false,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("contractId"));
    assert!(json.contains("startTime"));
    assert!(json.contains("endTime"));
    assert!(json.contains("unitNumber"));
    assert!(json.contains("includePartialBar"));
}
