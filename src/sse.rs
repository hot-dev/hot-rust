use serde_json::Value;

use crate::StreamEvent;

/// Parses the complete SSE blocks in `buffer` and returns the decoded events
/// plus the unconsumed remainder.
pub(crate) fn consume_sse_blocks(buffer: &str) -> (Vec<StreamEvent>, String) {
    let mut events = Vec::new();
    let mut remaining = buffer;

    while let Some((block, rest)) = remaining.split_once("\n\n") {
        remaining = rest;

        let mut data = String::new();
        let mut has_data = false;
        for line in block.split('\n') {
            let line = line.strip_suffix('\r').unwrap_or(line);
            if let Some(payload) = line.strip_prefix("data: ") {
                data.push_str(payload);
                has_data = true;
            } else if let Some(payload) = line.strip_prefix("data:") {
                data.push_str(payload);
                has_data = true;
            }
        }
        if !has_data {
            continue;
        }

        let Ok(parsed) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        if let Value::Object(object) = parsed {
            if object.get("type").is_some_and(Value::is_string) {
                events.push(object);
            }
        }
    }

    (events, remaining.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_partial_block() {
        let (events, remaining) = consume_sse_blocks("data: {\"type\":\"a\"}\n\ndata: {\"ty");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "a");
        assert_eq!(remaining, "data: {\"ty");
    }

    #[test]
    fn handles_data_without_space() {
        let (events, _) = consume_sse_blocks("data:{\"type\":\"a\"}\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "a");
    }

    #[test]
    fn joins_multiline_data() {
        let (events, _) = consume_sse_blocks("data: {\"type\":\ndata: \"a\"}\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "a");
    }

    #[test]
    fn skips_invalid_and_untyped_payloads() {
        let input = "data: not json\n\ndata: {\"no_type\":true}\n\n: comment\n\ndata: {\"type\":\"ok\"}\n\n";
        let (events, _) = consume_sse_blocks(input);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "ok");
    }

    #[test]
    fn handles_crlf_lines() {
        let (events, _) = consume_sse_blocks("data: {\"type\":\"a\"}\r\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "a");
    }
}
