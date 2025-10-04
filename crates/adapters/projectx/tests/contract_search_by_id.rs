use projectx::http::models::ContractSearchByIdReq;

#[test]
fn contract_search_by_id_serializes_camel_case() {
    let req = ContractSearchByIdReq {
        contract_id: "CON.F.US.ENQ.H25".to_string(),
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("contractId"));
    assert!(json.contains("CON.F.US.ENQ.H25"));
}
