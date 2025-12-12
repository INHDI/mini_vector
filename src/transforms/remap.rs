use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use vrl::compiler::{self, runtime::Runtime, state::RuntimeState, TargetValueRef, TimeZone};
use vrl::stdlib;
use vrl::value::{KeyString, Secrets, Value as VrlValue};
use std::collections::BTreeMap;

use crate::event::{Event, Value, LogEvent};
use crate::transforms::Transform;

pub struct RemapTransform {
    name: String,
    program: vrl::compiler::Program,
    runtime: Runtime,
}

impl RemapTransform {
    pub fn new(name: String, source: String) -> Result<Self> {
        let compiled = compiler::compile(&source, &stdlib::all())
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        Ok(Self {
            name,
            program: compiled.program,
            runtime: Runtime::new(RuntimeState::default()),
        })
    }

    fn process_one(&mut self, event: &mut Event) -> Result<()> {
        let mut v = match event {
            Event::Log(LogEvent { fields }) => {
                // map log fields to VRL object
                let obj: BTreeMap<KeyString, VrlValue> = fields
                    .iter()
                    .map(|(k, v)| (k.clone().into(), to_vrl_value(v)))
                    .collect();
                VrlValue::Object(obj)
            }
        };

        let mut metadata = VrlValue::Object(BTreeMap::new());
        let mut secrets = Secrets::default();
        let mut target = TargetValueRef {
            value: &mut v,
            metadata: &mut metadata,
            secrets: &mut secrets,
        };
        let tz = TimeZone::default();

        self.runtime
            .resolve(&mut target, &self.program, &tz)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // convert back to Event
        let Event::Log(LogEvent { fields }) = event;
        fields.clear();
        if let VrlValue::Object(obj) = v {
            for (k, v2) in obj {
                fields.insert(k.to_string(), from_vrl_value(&v2));
            }
        }

        Ok(())
    }
}

fn to_vrl_value(v: &Value) -> VrlValue {
    match v {
        Value::Null => VrlValue::Null,
        Value::String(s) => VrlValue::from(s.clone()),
        Value::Integer(i) => VrlValue::from(*i),
        Value::Float(f) => VrlValue::from_f64_or_zero(*f),
        Value::Bool(b) => VrlValue::from(*b),
        Value::Timestamp(ts) => VrlValue::from(*ts),
        Value::Array(arr) => VrlValue::Array(arr.iter().map(to_vrl_value).collect()),
        Value::Object(obj) => {
            let mut map = BTreeMap::new();
            for (k, v) in obj {
                map.insert(k.clone().into(), to_vrl_value(v));
            }
            VrlValue::Object(map)
        }
        Value::Bytes(b) => VrlValue::Bytes(b.clone().into()),
    }
}

fn from_vrl_value(v: &VrlValue) -> Value {
    match v {
        VrlValue::Null => Value::Null,
        VrlValue::Boolean(b) => Value::Bool(*b),
        VrlValue::Integer(i) => Value::Integer(*i),
        VrlValue::Float(f) => Value::Float(f.into_inner()), // ordered_float::NotNan
        VrlValue::Timestamp(ts) => Value::Timestamp(*ts),
        VrlValue::Bytes(b) => {
            if let Ok(s) = std::str::from_utf8(b) {
                Value::String(s.to_string())
            } else {
                Value::Bytes(b.to_vec())
            }
        },
        VrlValue::Regex(r) => Value::String(r.as_str().to_string()), // fallback
        VrlValue::Array(arr) => Value::Array(arr.iter().map(from_vrl_value).collect()),
        VrlValue::Object(obj) => {
            let mut map = BTreeMap::new();
            for (k, v) in obj {
                map.insert(k.to_string(), from_vrl_value(v));
            }
            Value::Object(map)
        }
    }
}

#[async_trait]
impl Transform for RemapTransform {
    async fn run(mut self: Box<Self>, mut input: mpsc::Receiver<Event>, output: mpsc::Sender<Event>) {
        while let Some(mut event) = input.recv().await {
            if let Err(err) = self.process_one(&mut event) {
                // policy: log warning and continue (drop event? or pass through unmodified?)
                // The user's code seemed to suggest dropping or warning. 
                // "tuỳ policy: log warning và drop, hay giữ nguyên event" -> "depends on policy: log warning and drop, or keep event"
                // The original code had `continue` which means drop.
                tracing::warn!(target = "transform.remap", name = %self.name, %err, "VRL transform failed");
                continue;
            }
            if output.send(event).await.is_err() {
                break;
            }
        }
    }
}
