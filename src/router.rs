use serde::Deserialize;

use crate::event::Event;

#[derive(Debug, Clone, Deserialize)]
pub struct RouteRuleConfig {
    pub equals: String,
    pub sink: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouterConfig {
    pub field: String,
    pub routes: Vec<RouteRuleConfig>,
}

pub struct EventRouter {
    field: String,
    rules: Vec<RouteRuleConfig>,
}

impl EventRouter {
    pub fn new(cfg: RouterConfig) -> Self {
        Self {
            field: cfg.field,
            rules: cfg.routes,
        }
    }

    pub fn route(&self, event: &Event) -> Vec<String> {
        let mut result = Vec::new();
        let v_opt = event.get_str(&self.field);
        for rule in &self.rules {
            if let Some(v) = v_opt {
                if v == rule.equals {
                    result.push(rule.sink.clone());
                }
            }
        }
        result
    }
}
