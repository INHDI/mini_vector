use crate::event::Event;

pub mod add_field;
pub mod contains_filter;
pub mod json_parse;
pub mod normalize_schema;
pub mod regex_parse;
pub mod script;

/// Returns false if the event should be dropped.
pub trait Transform: Send + Sync {
    fn apply(&self, event: &mut Event) -> bool;
}
