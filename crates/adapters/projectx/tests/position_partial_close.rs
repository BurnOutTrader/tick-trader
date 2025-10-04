use projectx::http::models::PartialCloseContractReq;

#[test]
fn partial_close_contract_req_serializes_camel_case_with_size() {
    let req = PartialCloseContractReq {
        account_id: 536,
        contract_id: "CON.F.US.GMET.J25".to_string(),
        size: 1,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"contractId\""));
    assert!(json.contains("\"size\""));
    assert!(json.contains("536"));
    assert!(json.contains("CON.F.US.GMET.J25"));
}
