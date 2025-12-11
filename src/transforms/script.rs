use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::event::{Event, Value};
use crate::transforms::Transform;

#[derive(Debug, Clone)]
pub enum Condition {
    Equals { left_field: String, right_literal: Option<String>, right_field: Option<String> },
    NotEquals { left_field: String, right_literal: Option<String>, right_field: Option<String> },
    // We can add more later
}

#[derive(Debug, Clone)]
pub enum ScriptOp {
    AssignLiteral { field: String, value: String },
    AssignField { field: String, src: String },
    UpcaseField { field: String, src: String },
    ToInt { field: String, src: String },
    Split { field: String, src: String, delim: String },
    DropEvent,
    If {
        condition: Condition,
        then_ops: Vec<ScriptOp>,
        else_ops: Vec<ScriptOp>,
    },
}

pub struct ScriptTransform {
    pub name: String,
    pub ops: Vec<ScriptOp>,
}

impl ScriptTransform {
    pub fn compile(name: String, script: String) -> Result<Self> {
        let lines: Vec<&str> = script.lines().map(|l| l.trim()).filter(|l| !l.is_empty() && !l.starts_with('#')).collect();
        let mut cursor = 0;
        let ops = Self::parse_block(&lines, &mut cursor)?;
        Ok(Self { name, ops })
    }

    fn parse_block(lines: &[&str], cursor: &mut usize) -> Result<Vec<ScriptOp>> {
        let mut ops = Vec::new();
        
        while *cursor < lines.len() {
            let line = lines[*cursor];
            
            if line == "}" {
                *cursor += 1;
                return Ok(ops);
            }
            
            if line == "} else {" {
                // Return to parent, do NOT consume here. 
                // Parent handles the transition from "then" block to "else" block.
                return Ok(ops);
            }

            if line.starts_with("if ") {
                *cursor += 1;
                let content = line.trim_start_matches("if ").trim_end_matches(" {");
                let condition = Self::parse_condition(content)?;
                
                let then_ops = Self::parse_block(lines, cursor)?;
                
                let mut else_ops = Vec::new();
                
                // After parse_block returns, cursor is at next token.
                // We need to check if we just finished a block that was followed by "else {"
                // Or if parse_block returned because it hit "} else {"
                
                // Let's look back one line? No.
                // Let's look at current line?
                if *cursor < lines.len() {
                    let current = lines[*cursor];
                    if current == "else {" {
                        *cursor += 1;
                        else_ops = Self::parse_block(lines, cursor)?;
                    } else if current == "} else {" {
                         // This shouldn't happen if parse_block returns on it?
                         // Wait, if parse_block returned on "} else {", it didn't consume it.
                         // So cursor points to "} else {".
                         *cursor += 1;
                         else_ops = Self::parse_block(lines, cursor)?;
                    }
                }
                
                // If the previous block ended with "}", check if next is "else {"
                // My logic in `parse_block` consumes "}".
                // So if we had:
                // if cond {
                //   ...
                // } else {
                // 
                // parse_block consumes "}", returns. cursor points to "else {".
                
                ops.push(ScriptOp::If { condition, then_ops, else_ops });
                continue;
            }
            
            // Standard Ops
            if line == "drop()" {
                ops.push(ScriptOp::DropEvent);
                *cursor += 1;
                continue;
            }

            // Assignments
            if let Some(op) = Self::parse_assignment(line)? {
                ops.push(op);
                *cursor += 1;
                continue;
            }
            
            anyhow::bail!("Unknown syntax: {}", line);
        }
        
        Ok(ops)
    }

    fn parse_condition(s: &str) -> Result<Condition> {
        // Simple parser: .field == "literal" or .field == .other
        // Note: Check != first because == is substring of != if we are not careful?
        // But we split by string.
        
        if s.contains("!=") {
             let parts: Vec<&str> = s.split("!=").collect();
             if parts.len() == 2 {
                 let left = parts[0].trim();
                let right = parts[1].trim();
                
                if !left.starts_with('.') {
                    anyhow::bail!("Condition left side must be a field: {}", left);
                }
                let left_field = left[1..].to_string();
                
                if right.starts_with('"') && right.ends_with('"') {
                    return Ok(Condition::NotEquals {
                        left_field,
                        right_literal: Some(right[1..right.len()-1].to_string()),
                        right_field: None,
                    });
                } else if right.starts_with('.') {
                     return Ok(Condition::NotEquals {
                        left_field,
                        right_literal: None,
                        right_field: Some(right[1..].to_string()),
                    });
                }
            }
        }
        
        if s.contains("==") {
            let parts: Vec<&str> = s.split("==").collect();
            if parts.len() == 2 {
                let left = parts[0].trim();
                let right = parts[1].trim();
                
                if !left.starts_with('.') {
                    anyhow::bail!("Condition left side must be a field: {}", left);
                }
                let left_field = left[1..].to_string();
                
                if right.starts_with('"') && right.ends_with('"') {
                    return Ok(Condition::Equals {
                        left_field,
                        right_literal: Some(right[1..right.len()-1].to_string()),
                        right_field: None,
                    });
                } else if right.starts_with('.') {
                     return Ok(Condition::Equals {
                        left_field,
                        right_literal: None,
                        right_field: Some(right[1..].to_string()),
                    });
                }
            }
        }

        anyhow::bail!("Unsupported condition: {}", s);
    }

    fn parse_assignment(line: &str) -> Result<Option<ScriptOp>> {
        if !line.contains('=') {
            return Ok(None);
        }
        let parts: Vec<&str> = line.splitn(2, '=').collect();
        let left = parts[0].trim();
        let right = parts[1].trim();

        if !left.starts_with('.') {
            return Ok(None);
        }
        let field_name = left[1..].to_string();

        // .field = "literal"
        if right.starts_with('"') && right.ends_with('"') {
            return Ok(Some(ScriptOp::AssignLiteral {
                field: field_name,
                value: right[1..right.len() - 1].to_string(),
            }));
        }

        // .field = upcase(.src)
        if right.starts_with("upcase(") && right.ends_with(')') {
            let inner = right["upcase(".len()..right.len()-1].trim();
            if inner.starts_with('.') {
                return Ok(Some(ScriptOp::UpcaseField {
                    field: field_name,
                    src: inner[1..].to_string(),
                }));
            }
        }

        // .field = to_int(.src)
        if right.starts_with("to_int(") && right.ends_with(')') {
            let inner = right["to_int(".len()..right.len()-1].trim();
            if inner.starts_with('.') {
                return Ok(Some(ScriptOp::ToInt {
                    field: field_name,
                    src: inner[1..].to_string(),
                }));
            }
        }

        // .field = split(.src, "delim")
        if right.starts_with("split(") && right.ends_with(')') {
             let inner = right["split(".len()..right.len()-1].trim();
             // Expected: .src, "delim"
             // splitn(2, ',') might fail if the delimiter itself contains commas, but for simplicity:
             let args: Vec<&str> = inner.splitn(2, ',').collect();
             if args.len() == 2 {
                 let src_arg = args[0].trim();
                 let delim_arg = args[1].trim();
                 
                 if src_arg.starts_with('.') && delim_arg.starts_with('"') && delim_arg.ends_with('"') {
                      return Ok(Some(ScriptOp::Split {
                         field: field_name,
                         src: src_arg[1..].to_string(),
                         delim: delim_arg[1..delim_arg.len()-1].to_string(),
                      }));
                 }
             }
        }

        // .field = .other
        if right.starts_with('.') {
            return Ok(Some(ScriptOp::AssignField {
                field: field_name,
                src: right[1..].to_string(),
            }));
        }

        anyhow::bail!("Invalid assignment: {}", line);
    }

    fn check_condition(&self, cond: &Condition, log: &crate::event::LogEvent) -> bool {
        match cond {
            Condition::Equals { left_field, right_literal, right_field } => {
                let left_val = log.fields.get(left_field);
                if let Some(lit) = right_literal {
                    match left_val {
                        Some(Value::String(s)) => s == lit,
                        _ => false,
                    }
                } else if let Some(rf) = right_field {
                    let right_val = log.fields.get(rf);
                    left_val == right_val
                } else {
                    false
                }
            }
            Condition::NotEquals { left_field, right_literal, right_field } => {
                 let left_val = log.fields.get(left_field);
                if let Some(lit) = right_literal {
                    match left_val {
                        Some(Value::String(s)) => {
                             // s != lit
                             s != lit
                        }
                        _ => {
                             // Missing or not string => not equal to literal string => True
                             true 
                        }
                    }
                } else if let Some(rf) = right_field {
                    let right_val = log.fields.get(rf);
                    left_val != right_val
                } else {
                    true
                }
            }
        }
    }

    fn apply_ops(&self, ops: &[ScriptOp], event: &mut Event) -> bool {
        // Only log events supported
        if event.as_log().is_none() {
            return true;
        }

        for op in ops {
            match op {
                ScriptOp::AssignLiteral { field, value } => {
                    if let Some(log) = event.as_log_mut() {
                        log.fields.insert(field.clone(), Value::String(value.clone()));
                    }
                }
                ScriptOp::AssignField { field, src } => {
                    if let Some(log) = event.as_log_mut() {
                        if let Some(v) = log.fields.get(src).cloned() {
                            log.fields.insert(field.clone(), v);
                        }
                    }
                }
                ScriptOp::UpcaseField { field, src } => {
                    if let Some(log) = event.as_log_mut() {
                        let val = log.fields.get(src).cloned();
                        if let Some(Value::String(s)) = val {
                            log.fields.insert(field.clone(), Value::String(s.to_uppercase()));
                        }
                    }
                }
                ScriptOp::ToInt { field, src } => {
                    if let Some(log) = event.as_log_mut() {
                        let val = log.fields.get(src).cloned();
                        if let Some(v) = val {
                            match v {
                                Value::String(s) => {
                                    if let Ok(i) = s.parse::<i64>() {
                                        log.fields.insert(field.clone(), Value::Integer(i));
                                    }
                                }
                                Value::Integer(i) => {
                                    log.fields.insert(field.clone(), Value::Integer(i));
                                }
                                _ => {}
                            }
                        }
                    }
                }
                ScriptOp::Split { field, src, delim } => {
                    if let Some(log) = event.as_log_mut() {
                        let val = log.fields.get(src).cloned();
                        if let Some(Value::String(s)) = val {
                            let parts: Vec<Value> = s.split(delim)
                                .map(|p| Value::String(p.to_string()))
                                .collect();
                            log.fields.insert(field.clone(), Value::Array(parts));
                        }
                    }
                }
                ScriptOp::DropEvent => return false,
                ScriptOp::If { condition, then_ops, else_ops } => {
                    let cond_true = {
                        let log = event.as_log().unwrap(); 
                        self.check_condition(condition, log)
                    };
                    
                    if cond_true {
                        if !self.apply_ops(then_ops, event) {
                            return false;
                        }
                    } else {
                        if !self.apply_ops(else_ops, event) {
                             return false;
                        }
                    }
                }
            }
        }
        true
    }
}

#[async_trait]
impl Transform for ScriptTransform {
    async fn run(self: Box<Self>, mut input: mpsc::Receiver<Event>, output: mpsc::Sender<Event>) {
        while let Some(mut event) = input.recv().await {
            metrics::increment_counter!("events_in", "component" => self.name.clone());
            
            if self.apply_ops(&self.ops, &mut event) {
                 if output.send(event).await.is_err() { break; }
                 metrics::increment_counter!("events_out", "component" => self.name.clone());
            } else {
                 metrics::increment_counter!("events_dropped", "component" => self.name.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_script_logic() {
        // .a = "1"
        // if .a == "1" {
        //     .b = "matched"
        // } else {
        //     .b = "ignored"
        // }
        // 
        // if .a != "1" {
        //     .c = "wrong"
        // } else {
        //     .c = "correct"
        // }
        
        let script = r#"
        .a = "1"
        if .a == "1" {
            .b = "matched"
        } else {
            .b = "ignored"
        }
        
        if .a != "1" {
            .c = "wrong"
        } else {
            .c = "correct"
        }
        
        .num = "123"
        .num_int = to_int(.num)
        "#;
        
        let t = ScriptTransform::compile("test".into(), script.to_string()).expect("compile");
        let event = Event::new();
        // .a = "1" is done in the script, so we start with empty or pre-populated?
        // Wait, the script says `.a = "1"` at the top.
        // So we don't need to populate .a manually.
        
        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, mut rx_out) = mpsc::channel(1);
        tx_in.send(event).await.unwrap();
        drop(tx_in);

        Box::new(t).run(rx_in, tx_out).await;
        
        let event = rx_out.recv().await.expect("should output");
        let log = event.as_log().unwrap();
        
        assert_eq!(log.fields.get("b"), Some(&Value::String("matched".to_string())));
        // .c should be "correct" because .a ("1") == "1", so != "1" is FALSE.
        // Wait: .a == "1" -> True -> .b = "matched".
        // .a != "1" -> False -> Else -> .c = "correct".
        // The failure says left: None.
        // This implies .c was never set.
        // Debugging the script execution flow might be needed.
        // It seems the parser might be consuming lines incorrectly or the condition check fails silently?
        // Or "if .a != "1"" is not parsed correctly.
        assert_eq!(log.fields.get("c"), Some(&Value::String("correct".to_string())));
        assert_eq!(log.fields.get("num_int"), Some(&Value::Integer(123)));
    }
}
