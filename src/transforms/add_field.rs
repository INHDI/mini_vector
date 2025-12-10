use crate::event::{Event, Value};
use crate::transforms::Transform;

pub struct AddFieldTransform {
    pub field: String,
    pub value: Value,
}

impl AddFieldTransform {
    pub fn new(field: String, value: Value) -> Self {
        Self { field, value }
    }
}

impl Transform for AddFieldTransform {
    fn apply(&self, event: &mut Event) -> bool {
        event.insert(self.field.clone(), self.value.clone());
        true
    }
}
