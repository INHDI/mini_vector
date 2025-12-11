use std::collections::BTreeMap;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::error;

use crate::event::{Event, Value};
use crate::transforms::Transform;

// VRL
use vrl::value::{Bytes as VrlBytes, KeyString, Value as VrlValue};

/// Tạm thời: struct remap chỉ là khung.
/// Bước sau mới nối compile / runtime của VRL vào.
pub struct RemapTransform {
    /// Source VRL script (nội dung field `source` trong mini_vector.yml)
    pub source: String,
}

impl RemapTransform {
    pub fn new(source: String) -> Self {
        Self { source }
    }

    /// Xử lý 1 event – hiện tại chỉ là no-op để fix compile.
    /// Sau này mình sẽ:
    ///   1. Convert Event -> VrlValue (object)
    ///   2. Chạy VRL program
    ///   3. Convert VrlValue -> Event
    fn process_one(&mut self, _event: &mut Event) -> anyhow::Result<()> {
        // TODO: tích hợp VRL compiler + runtime ở đây
        Ok(())
    }
}

#[async_trait]
impl Transform for RemapTransform {
    async fn run(
        mut self: Box<Self>,
        mut input: mpsc::Receiver<Event>,
        output: mpsc::Sender<Event>,
    ) {
        while let Some(mut event) = input.recv().await {
            if let Err(err) = self.process_one(&mut event) {
                error!(target: "remap", "remap error: {err}");
                // giống Vector: log lỗi rồi bỏ event đó
                continue;
            }

            if output.send(event).await.is_err() {
                break;
            }
        }
    }
}

/// Convert mini_vector::Value -> vrl::value::Value
fn to_vrl_value(value: &Value) -> VrlValue {
    match value {
        Value::Null => VrlValue::Null,
        Value::String(s) => VrlValue::from(s.clone()),
        Value::Integer(i) => VrlValue::from(*i),
        Value::Float(f) => VrlValue::from_f64_or_zero(*f),
        Value::Bool(b) => VrlValue::from(*b),
        Value::Timestamp(ts) => VrlValue::from(ts.clone()),
        Value::Array(arr) => {
            let inner: Vec<VrlValue> = arr.iter().map(to_vrl_value).collect();
            VrlValue::from(inner)
        }
        Value::Object(map) => {
            let mut obj: BTreeMap<KeyString, VrlValue> = BTreeMap::new();
            for (k, v) in map {
                obj.insert(KeyString::from(k.as_str()), to_vrl_value(v));
            }
            VrlValue::from(obj)
        }
        Value::Bytes(bytes) => {
            let b = VrlBytes::copy_from_slice(bytes);
            VrlValue::from(b)
        }
    }
}

/// Convert vrl::value::Value -> mini_vector::Value
fn from_vrl_value(v: &VrlValue) -> Value {
    match v {
        VrlValue::Null => Value::Null,
        VrlValue::Bytes(b) => Value::Bytes(b.to_vec()),
        VrlValue::Regex(_) => Value::String(v.to_string_lossy().into_owned()),
        VrlValue::Integer(i) => Value::Integer(*i),
        VrlValue::Float(f) => {
            // ordered_float::NotNan<f64> -> f64
            let f64_val: f64 = (*f).into();
            Value::Float(f64_val)
        }
        VrlValue::Boolean(b) => Value::Bool(*b),
        VrlValue::Timestamp(ts) => Value::Timestamp(*ts),
        VrlValue::Array(arr) => {
            let inner = arr.iter().map(from_vrl_value).collect();
            Value::Array(inner)
        }
        VrlValue::Object(obj) => {
            let mut map = BTreeMap::new();
            for (k, v2) in obj {
                map.insert(k.to_string(), from_vrl_value(v2));
            }
            Value::Object(map)
        }
    }
}
