use mini_vector::event::{Event, Value};
use mini_vector::transforms::json_parse::JsonParseTransform;
use mini_vector::transforms::normalize_schema::NormalizeSchemaTransform;
use tokio::sync::mpsc;

#[tokio::test]
async fn pipeline_like_flow_parses_and_normalizes() {
    // Simulate: json_parse -> normalize_schema
    let mut event = Event::new();
    event.insert(
        "message",
        r#"{"host":"srv1","program":"nginx","severity":"warning","log_type":"web","message":"started"}"#,
    );

    let json = JsonParseTransform::new("json_parse".into(), "message".into(), true, true, None);
    let norm = NormalizeSchemaTransform::new(
        "normalize".into(),
        None,
        Some("host".into()),
        Some("severity".into()),
        Some("program".into()),
        Some("message".into()),
        None,
    );

    let (tx_in, rx_in) = mpsc::channel(4);
    let (tx_mid, rx_mid) = mpsc::channel(4);
    let (tx_out, mut rx_out) = mpsc::channel(4);

    tokio::spawn(async move { Box::new(json).run(rx_in, tx_mid).await; });
    tokio::spawn(async move { Box::new(norm).run(rx_mid, tx_out).await; });

    tx_in.send(event).await.unwrap();
    drop(tx_in);

    let out_event = rx_out.recv().await.expect("should output event");

    assert!(matches!(out_event.as_log().unwrap().fields.get("@timestamp"), Some(Value::Timestamp(_))));
    assert_eq!(
        out_event.as_log().unwrap().fields.get("host"),
        Some(&Value::String("srv1".to_string()))
    );
    assert_eq!(
        out_event.as_log().unwrap().fields.get("program"),
        Some(&Value::String("nginx".to_string()))
    );
    assert_eq!(
        out_event.as_log().unwrap().fields.get("severity"),
        Some(&Value::String("WARN".to_string()))
    );
    assert_eq!(
        out_event.as_log().unwrap().fields.get("log_type"),
        Some(&Value::String("web".to_string()))
    );
}

#[tokio::test]
async fn pipeline_defaults_when_fields_missing() {
    let mut event = Event::new();
    event.insert("message", r#"{"message":"no ts"}"#);

    let json = JsonParseTransform::new("json_parse".into(), "message".into(), true, true, None);
    let norm = NormalizeSchemaTransform::new(
        "normalize".into(),
        Some("@timestamp".into()),
        Some("host".into()),
        Some("severity".into()),
        Some("program".into()),
        Some("message".into()),
        None,
    );

    let (tx_in, rx_in) = mpsc::channel(4);
    let (tx_mid, rx_mid) = mpsc::channel(4);
    let (tx_out, mut rx_out) = mpsc::channel(4);

    tokio::spawn(async move { Box::new(json).run(rx_in, tx_mid).await; });
    tokio::spawn(async move { Box::new(norm).run(rx_mid, tx_out).await; });

    tx_in.send(event).await.unwrap();
    drop(tx_in);

    let out_event = rx_out.recv().await.expect("should output event");

    assert!(matches!(out_event.as_log().unwrap().fields.get("@timestamp"), Some(Value::Timestamp(_))));
    assert_eq!(
        out_event.as_log().unwrap().fields.get("log_type"),
        Some(&Value::String("unknown".to_string()))
    );
    assert_eq!(
        out_event.as_log().unwrap().fields.get("severity"),
        Some(&Value::String("INFO".to_string()))
    );
    assert_eq!(
        out_event.as_log().unwrap().fields.get("soc_tenant"),
        Some(&Value::String("unknown".to_string()))
    );
}
