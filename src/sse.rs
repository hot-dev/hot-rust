use serde_json::Value;

use crate::{Error, Result, StreamEvent};

/// Maximum buffered SSE size before aborting the stream to bound memory.
pub(crate) const MAX_SSE_BUFFER_BYTES: usize = 16 * 1024 * 1024;

/// Decodes UTF-8 bytes from `pending` into `buffer`.
///
/// Invalid byte sequences are replaced with `U+FFFD` so a malformed byte never
/// stalls the decoder. An incomplete trailing multi-byte UTF-8 sequence stays
/// in `pending` until more bytes arrive, unless `eof` is true, in which case
/// it is replaced with `U+FFFD` as well.
pub(crate) fn decode_utf8_pending(pending: &mut Vec<u8>, buffer: &mut String, eof: bool) {
    loop {
        match std::str::from_utf8(pending) {
            Ok(valid) => {
                buffer.push_str(valid);
                pending.clear();
                break;
            }
            Err(error) => {
                let valid_up_to = error.valid_up_to();
                if valid_up_to > 0 {
                    if let Ok(valid) = std::str::from_utf8(&pending[..valid_up_to]) {
                        buffer.push_str(valid);
                    }
                    pending.drain(..valid_up_to);
                }
                match error.error_len() {
                    Some(bad_len) => {
                        buffer.push('\u{FFFD}');
                        pending.drain(..bad_len);
                    }
                    None => {
                        if eof && !pending.is_empty() {
                            buffer.push('\u{FFFD}');
                            pending.clear();
                        }
                        break;
                    }
                }
            }
        }
    }
}

/// Streaming byte-to-event SSE decoder with bounded buffering.
pub(crate) struct SseDecoder {
    pending: Vec<u8>,
    buffer: String,
    started: bool,
    max_buffer_bytes: usize,
}

impl SseDecoder {
    pub(crate) fn new() -> Self {
        Self::with_max_buffer_bytes(MAX_SSE_BUFFER_BYTES)
    }

    pub(crate) fn with_max_buffer_bytes(max_buffer_bytes: usize) -> Self {
        Self {
            pending: Vec::new(),
            buffer: String::new(),
            started: false,
            max_buffer_bytes,
        }
    }

    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<Vec<StreamEvent>> {
        if self
            .buffer
            .len()
            .saturating_add(self.pending.len())
            .saturating_add(chunk.len())
            > self.max_buffer_bytes
        {
            return Err(buffer_limit_error(self.max_buffer_bytes));
        }
        self.pending.extend_from_slice(chunk);
        decode_utf8_pending(&mut self.pending, &mut self.buffer, false);
        self.strip_initial_bom();
        consume_sse_blocks_inner(&mut self.buffer, false, false, self.max_buffer_bytes)
    }

    pub(crate) fn finish(&mut self) -> Result<Vec<StreamEvent>> {
        decode_utf8_pending(&mut self.pending, &mut self.buffer, true);
        self.strip_initial_bom();
        consume_sse_blocks_inner(&mut self.buffer, false, true, self.max_buffer_bytes)
    }

    fn strip_initial_bom(&mut self) {
        if !self.started && !self.buffer.is_empty() {
            if self.buffer.starts_with('\u{FEFF}') {
                self.buffer.drain(..'\u{FEFF}'.len_utf8());
            }
            self.started = true;
        }
    }
}

/// Parses complete SSE blocks in `buffer`, draining consumed bytes in-place to
/// avoid O(N²) reallocation.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn consume_sse_blocks(buffer: &mut String) -> Result<Vec<StreamEvent>> {
    consume_sse_blocks_inner(buffer, true, false, MAX_SSE_BUFFER_BYTES)
}

fn consume_sse_blocks_inner(
    buffer: &mut String,
    strip_bom: bool,
    eof: bool,
    max_buffer_bytes: usize,
) -> Result<Vec<StreamEvent>> {
    if buffer.len() > max_buffer_bytes {
        return Err(buffer_limit_error(max_buffer_bytes));
    }
    if strip_bom && buffer.starts_with('\u{FEFF}') {
        buffer.drain(..'\u{FEFF}'.len_utf8());
    }

    let mut events = Vec::new();
    let mut consumed = 0;

    while let Some((block_end, next_start)) = find_block_end(&buffer[consumed..], eof) {
        let block = &buffer[consumed..consumed + block_end];
        consumed += next_start;
        if let Some(event) = parse_sse_block(block) {
            events.push(event);
        }
    }

    if consumed > 0 {
        buffer.drain(..consumed);
    }

    if buffer.len() > max_buffer_bytes {
        return Err(buffer_limit_error(max_buffer_bytes));
    }

    Ok(events)
}

fn buffer_limit_error(max_buffer_bytes: usize) -> Error {
    Error::Protocol(format!(
        "SSE stream buffer exceeded maximum size of {max_buffer_bytes} bytes"
    ))
}

/// Finds the first complete SSE block terminated by a blank line (`\n\n`,
/// `\r\n\r\n`, `\r\r`, or mixed CRLF/LF). Returns `(block_end, next_start)`
/// offsets into `input`.
fn find_block_end(input: &str, eof: bool) -> Option<(usize, usize)> {
    let bytes = input.as_bytes();
    let mut line_start = 0;
    let mut prev_ended_cr_only = false;

    while line_start < bytes.len() {
        let mut eol = line_start;
        while eol < bytes.len() && bytes[eol] != b'\n' && bytes[eol] != b'\r' {
            eol += 1;
        }
        if eol == bytes.len() {
            return None;
        }

        let (next_line_start, ended_cr_only) = if bytes[eol] == b'\n' {
            (eol + 1, false)
        } else if eol + 1 < bytes.len() {
            if bytes[eol + 1] == b'\n' {
                (eol + 2, false)
            } else {
                (eol + 1, true)
            }
        } else if eol == line_start && (prev_ended_cr_only || eof) {
            // Complete CR-only blank line (`\r\r`), or blank CR line at EOF.
            (eol + 1, true)
        } else {
            // Trailing `\r` may be the first half of a `\r\n` split across chunks.
            return None;
        };

        if eol == line_start {
            return Some((line_start, next_line_start));
        }
        line_start = next_line_start;
        prev_ended_cr_only = ended_cr_only;
    }

    None
}

/// Extracts and joins `data:` field payloads per WHATWG SSE §9.2.6 (`'\n'`
/// between consecutive `data:` lines).
fn extract_sse_data(block: &str) -> Option<String> {
    let mut data = String::new();
    let mut has_data = false;
    let bytes = block.as_bytes();
    let mut pos = 0;

    while pos < bytes.len() {
        let line_start = pos;
        while pos < bytes.len() && bytes[pos] != b'\n' && bytes[pos] != b'\r' {
            pos += 1;
        }
        let line = &block[line_start..pos];
        if pos < bytes.len() {
            if bytes[pos] == b'\r' && pos + 1 < bytes.len() && bytes[pos + 1] == b'\n' {
                pos += 2;
            } else {
                pos += 1;
            }
        }
        let payload = if let Some(payload) = line.strip_prefix("data: ") {
            Some(payload)
        } else {
            line.strip_prefix("data:")
        };
        if let Some(payload) = payload {
            if has_data {
                data.push('\n');
            }
            data.push_str(payload);
            has_data = true;
        }
    }

    if has_data {
        Some(data)
    } else {
        None
    }
}

fn parse_sse_block(block: &str) -> Option<StreamEvent> {
    let data = extract_sse_data(block)?;
    let parsed = serde_json::from_str::<Value>(&data).ok()?;
    if let Value::Object(object) = parsed {
        if object.get("type").is_some_and(Value::is_string) {
            return Some(object);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_partial_block() {
        let mut buffer = "data: {\"type\":\"a\"}\n\ndata: {\"ty".to_string();
        let events = consume_sse_blocks(&mut buffer).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "a");
        assert_eq!(buffer, "data: {\"ty");
    }

    #[test]
    fn handles_data_without_space() {
        let mut buffer = "data:{\"type\":\"a\"}\n\n".to_string();
        let events = consume_sse_blocks(&mut buffer).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "a");
        assert!(buffer.is_empty());
    }

    #[test]
    fn joins_multiline_data_with_newline() {
        assert_eq!(
            extract_sse_data("data: line1\ndata: line2\ndata:\ndata: line4").as_deref(),
            Some("line1\nline2\n\nline4")
        );

        let mut buffer = "data: {\"type\":\ndata: \"a\"}\n\n".to_string();
        let events = consume_sse_blocks(&mut buffer).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "a");

        // Because `data:` lines are joined with `\n`, splitting inside a JSON
        // string produces an unescaped literal newline (invalid JSON) rather
        // than silently concatenating the fragments.
        let mut split_string =
            "data: {\"type\":\"a\",\"s\":\"hello\ndata: world\"}\n\n".to_string();
        let events = consume_sse_blocks(&mut split_string).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn skips_invalid_and_untyped_payloads() {
        let mut buffer =
            "data: not json\n\ndata: {\"no_type\":true}\n\n: comment\n\ndata: {\"type\":\"ok\"}\n\n"
                .to_string();
        let events = consume_sse_blocks(&mut buffer).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "ok");
    }

    #[test]
    fn handles_crlf_lines() {
        let mut buffer = "data: {\"type\":\"a\"}\r\n\r\ndata: {\"type\":\"b\"}\r\n\r\n".to_string();
        let events = consume_sse_blocks(&mut buffer).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], "a");
        assert_eq!(events[1]["type"], "b");
        assert!(buffer.is_empty());
    }

    #[test]
    fn handles_crlf_split_across_chunks() {
        let mut decoder = SseDecoder::new();
        assert!(decoder
            .push(b"data: {\"type\":\"a\"}\r\n\r")
            .unwrap()
            .is_empty());
        let events = decoder.push(b"\ndata: {\"type\":\"b\"}\r\n\r\n").unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], "a");
        assert_eq!(events[1]["type"], "b");
    }

    #[test]
    fn handles_cr_only_delimiters() {
        let mut buffer = "data: {\"type\":\rdata: \"a\"}\r\rdata: {\"type\":\"b\"}\r\r".to_string();
        let events = consume_sse_blocks(&mut buffer).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], "a");
        assert_eq!(events[1]["type"], "b");
        assert!(buffer.is_empty());
    }

    #[test]
    fn strips_leading_utf8_bom() {
        let mut buffer = "\u{FEFF}data: {\"type\":\"a\"}\n\n".to_string();
        let events = consume_sse_blocks(&mut buffer).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "a");

        // BOM split across chunks is still stripped once at stream start.
        let mut decoder = SseDecoder::new();
        assert!(decoder.push(&[0xEF, 0xBB]).unwrap().is_empty());
        let events = decoder.push(b"\xBFdata: {\"type\":\"bom\"}\n\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "bom");
    }

    #[test]
    fn decodes_multibyte_utf8_split_across_chunks() {
        let mut decoder = SseDecoder::new();
        // "€" = [0xE2, 0x82, 0xAC], "🎉" = [0xF0, 0x9F, 0x8E, 0x89]
        let payload = "data: {\"type\":\"chunk\",\"text\":\"€🎉\"}\n\n".as_bytes();
        let mut events = Vec::new();
        for byte in payload {
            events.extend(decoder.push(&[*byte]).unwrap());
        }
        events.extend(decoder.finish().unwrap());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["text"], "€🎉");
    }

    #[test]
    fn invalid_utf8_bytes_do_not_stall_or_grow_pending() {
        let mut decoder = SseDecoder::new();
        // First event contains an invalid 0xFF byte inside a JSON string;
        // decoder replaces it with U+FFFD and continues cleanly to later events.
        let mut chunk = Vec::new();
        chunk.extend_from_slice(b"data: {\"type\":\"bad\",\"text\":\"hi");
        chunk.push(0xFF);
        chunk.extend_from_slice(b"there\"}\n\ndata: {\"type\":\"after\"}\n\n");

        let events = decoder.push(&chunk).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], "bad");
        assert_eq!(events[0]["text"], "hi\u{FFFD}there");
        assert_eq!(events[1]["type"], "after");
        assert!(decoder.pending.is_empty());

        // Incomplete multi-byte sequence at EOF is flushed without stalling.
        assert!(decoder.push(&[0xE2, 0x82]).unwrap().is_empty());
        assert_eq!(decoder.pending.len(), 2);
        assert!(decoder.finish().unwrap().is_empty());
        assert!(decoder.pending.is_empty());
    }

    #[test]
    fn does_not_flush_unterminated_trailing_event_at_eof() {
        let mut decoder = SseDecoder::new();
        let events = decoder
            .push(b"data: {\"type\":\"complete\"}\n\ndata: {\"type\":\"truncated\"}\n")
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "complete");
        assert!(decoder.finish().unwrap().is_empty());
    }

    #[test]
    fn enforces_max_buffer_cap() {
        let mut decoder = SseDecoder::with_max_buffer_bytes(64);
        let err = decoder.push(&[b'a'; 65]).unwrap_err();
        assert!(matches!(err, Error::Protocol(msg) if msg.contains("maximum size")));

        let mut huge = "a".repeat(MAX_SSE_BUFFER_BYTES + 1);
        let err = consume_sse_blocks(&mut huge).unwrap_err();
        assert!(matches!(err, Error::Protocol(msg) if msg.contains("maximum size")));
    }
}
