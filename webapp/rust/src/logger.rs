use std::fmt::Display;
use std::io::Write;

use chrono::Utc;

pub fn error(msg: &str, err: &dyn Display) {
    write("ERROR", msg, Some(err.to_string()));
}

pub fn warn(msg: &str) {
    write("WARN", msg, None);
}

fn write(level: &str, msg: &str, err: Option<String>) {
    let quote = |value: &str| serde_json::to_string(value).unwrap_or_default();
    let mut line = format!(
        "{{\"time\":{},\"level\":{},\"msg\":{}",
        quote(&Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()),
        quote(level),
        quote(msg)
    );
    if let Some(err) = err {
        line.push_str(&format!(",\"error\":{}", quote(&err)));
    }
    line.push_str("}\n");
    let _ = std::io::stdout().lock().write_all(line.as_bytes());
}
