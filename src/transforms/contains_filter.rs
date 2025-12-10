use crate::event::Event;
use crate::transforms::Transform;

pub struct ContainsFilterTransform {
    pub field: String,
    pub needle: String,
}

impl ContainsFilterTransform {
    pub fn new(field: String, needle: String) -> Self {
        Self { field, needle }
    }
}

impl Transform for ContainsFilterTransform {
    fn apply(&self, event: &mut Event) -> bool {
        if let Some(v) = event.get_str(&self.field) {
            v.contains(&self.needle)
        } else {
            false
        }
    }
}
