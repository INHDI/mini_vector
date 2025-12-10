use mini_vector::event::{Event, Value};
use mini_vector::transforms::json_parse::JsonParseTransform;
use mini_vector::transforms::normalize_schema::NormalizeSchemaTransform;

#[test]
fn pipeline_like_flow_parses_and_normalizes() {
    // Simulate: json_parse -> normalize_schema
    let mut event = Event::new();
    event.insert(
        "message",
        r#"{"host":"srv1","program":"nginx","severity":"warning","log_type":"web","message":"started"}"#,
    );

    let json = JsonParseTransform::new("message".into(), true, true);
    assert!(json.apply(&mut event));

    let norm = NormalizeSchemaTransform::new(
        None,
        Some("host".into()),
        Some("severity".into()),
        Some("program".into()),
        Some("message".into()),
        None,
    );
    assert!(norm.apply(&mut event));

    assert!(matches!(event.fields.get("@timestamp"), Some(Value::Timestamp(_))));
    assert_eq!(
        event.fields.get("host"),
        Some(&Value::String("srv1".to_string()))
    );
    assert_eq!(
        event.fields.get("program"),
        Some(&Value::String("nginx".to_string()))
    );
    assert_eq!(
        event.fields.get("severity"),
        Some(&Value::String("WARN".to_string()))
    );
    assert_eq!(
        event.fields.get("log_type"),
        Some(&Value::String("web".to_string()))
    );
}

#[test]
fn pipeline_defaults_when_fields_missing() {
    let mut event = Event::new();
    event.insert("message", r#"{"message":"no ts"}"#);

    let json = JsonParseTransform::new("message".into(), true, true);
    assert!(json.apply(&mut event));

    let norm = NormalizeSchemaTransform::new(
        Some("@timestamp".into()),
        Some("host".into()),
        Some("severity".into()),
        Some("program".into()),
        Some("message".into()),
        None,
    );
    assert!(norm.apply(&mut event));

    assert!(matches!(event.fields.get("@timestamp"), Some(Value::Timestamp(_))));
    assert_eq!(
        event.fields.get("log_type"),
        Some(&Value::String("unknown".to_string()))
    );
    assert_eq!(
        event.fields.get("severity"),
        Some(&Value::String("INFO".to_string()))
    );
    assert_eq!(
        event.fields.get("soc_tenant"),
        Some(&Value::String("unknown".to_string()))
    );
}
