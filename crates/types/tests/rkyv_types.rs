use chrono::{NaiveDate, Utc};
use rust_decimal::Decimal;
use tt_types::rkyv_types::{DateTimeUtcDef, RkyvDateTimeUtc, RkyvDecimal};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, PartialEq, Eq)]
struct TestPayload {
    #[rkyv(with = RkyvDateTimeUtc)]
    dt: chrono::DateTime<chrono::Utc>,
    #[rkyv(with = RkyvDecimal)]
    dec: Decimal,
}

#[test]
fn roundtrip_datetime_and_decimal() {
    // 2025-01-02 03:04:05.123456789Z
    let naive = NaiveDate::from_ymd_opt(2025, 1, 2)
        .unwrap()
        .and_hms_nano_opt(3, 4, 5, 123_456_789)
        .unwrap();
    let dt = chrono::DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);

    let dec = Decimal::from_i128_with_scale(123_456_789_012_345i128, 6);

    let payload = TestPayload { dt, dec };

    // Serialize using rkyv
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&payload).expect("serialize");

    // Ensure alignment and access archived root
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(&bytes);
    let archived = rkyv::access::<ArchivedTestPayload, rkyv::rancor::Error>(&aligned[..])
        .expect("access archived root");

    // Inspect archived representation matches our adapter logic
    assert_eq!(archived.dt.secs, dt.timestamp());
    assert_eq!(archived.dt.nanos, dt.timestamp_subsec_nanos());

    assert_eq!(archived.dec.mantissa, payload.dec.mantissa());
    assert_eq!(archived.dec.scale, payload.dec.scale() as u32);

    // Deserialize back
    let de: TestPayload =
        rkyv::deserialize::<TestPayload, rkyv::rancor::Error>(archived).expect("deserialize");

    assert_eq!(de, payload);
}

#[test]
fn archived_values_change_when_inputs_change() {
    let naive1 = NaiveDate::from_ymd_opt(2000, 1, 1)
        .unwrap()
        .and_hms_nano_opt(0, 0, 0, 0)
        .unwrap();
    let dt1 = chrono::DateTime::<Utc>::from_naive_utc_and_offset(naive1, Utc);
    let dec1 = Decimal::from_i128_with_scale(42, 0);

    let naive2 = NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_nano_opt(0, 0, 1, 1)
        .unwrap();
    let dt2 = chrono::DateTime::<Utc>::from_naive_utc_and_offset(naive2, Utc);
    let dec2 = Decimal::from_i128_with_scale(-42, 3);

    let p1 = TestPayload { dt: dt1, dec: dec1 };
    let p2 = TestPayload { dt: dt2, dec: dec2 };

    let b1 = rkyv::to_bytes::<rkyv::rancor::Error>(&p1).unwrap();
    let b2 = rkyv::to_bytes::<rkyv::rancor::Error>(&p2).unwrap();

    let mut a1_buf = rkyv::util::AlignedVec::<16>::with_capacity(b1.len());
    a1_buf.extend_from_slice(&b1);
    let mut a2_buf = rkyv::util::AlignedVec::<16>::with_capacity(b2.len());
    a2_buf.extend_from_slice(&b2);

    let a1 = rkyv::access::<ArchivedTestPayload, rkyv::rancor::Error>(&a1_buf[..]).unwrap();
    let a2 = rkyv::access::<ArchivedTestPayload, rkyv::rancor::Error>(&a2_buf[..]).unwrap();

    assert_ne!((a1.dt.secs, a1.dt.nanos), (a2.dt.secs, a2.dt.nanos));
    assert_ne!(a1.dec.mantissa, a2.dec.mantissa);
    assert_ne!(a1.dec.scale, a2.dec.scale);
}

#[test]
fn datetime_def_from_parts_roundtrips() {
    // Validate that our From<DateTimeUtcDef> reconstructs the exact instant for a set of tricky values
    let cases: &[(i64, u32)] = &[
        (0, 0),                      // epoch
        (0, 1),                      // just after epoch
        (1, 0),                      // one second after epoch
        (-1, 0),                     // one second before epoch
        (-1, 999_999_999),           // last ns of the second before epoch
        (1_234_567_890, 123_456_789) // a recent large value
    ];

    for &(secs, nanos) in cases {
        let def = DateTimeUtcDef { secs, nanos };
        let dt: chrono::DateTime<Utc> = def.into();
        assert_eq!(dt.timestamp(), secs, "secs mismatch for case {:?}", (secs, nanos));
        assert_eq!(dt.timestamp_subsec_nanos(), nanos, "nanos mismatch for case {:?}", (secs, nanos));
    }
}
