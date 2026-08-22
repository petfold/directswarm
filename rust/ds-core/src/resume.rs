//! Resume-state sidecar for interrupted fetches.
//!
//! While fetching, the client periodically commits `<output>.ds-resume`
//! recording the root reference and the byte offset up to which the
//! output file's contents are flushed and chunk-verified. On restart
//! with the same root, the fetch truncates the output to that offset
//! and range-joins from there — verified leaf chunks are never
//! refetched (interior tree nodes are, which is cheap). The sidecar is
//! removed on completion.
//!
//! Format (text, one field per line):
//!
//! ```text
//! directswarm-resume v1
//! root=<64 hex chars>
//! offset=<decimal u64>
//! ```

const MAGIC: &str = "directswarm-resume v1";

/// Suffix appended to the output path to name the sidecar.
pub const SIDECAR_SUFFIX: &str = ".ds-resume";

/// Verified progress of a partially-fetched file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumeState {
    /// Root reference of the fetch this state belongs to.
    pub root: [u8; 32],
    /// Bytes of the output file that are flushed and chunk-verified.
    pub offset: u64,
}

impl ResumeState {
    /// Serialize to sidecar text.
    #[must_use]
    pub fn render(&self) -> String {
        let mut root_hex = String::with_capacity(64);
        for byte in self.root {
            root_hex.push(hex_digit(byte >> 4));
            root_hex.push(hex_digit(byte & 0x0f));
        }
        format!("{MAGIC}\nroot={root_hex}\noffset={}\n", self.offset)
    }

    /// Parse sidecar text. Returns `None` on any mismatch — an
    /// unreadable sidecar means "no resume", never an error.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let mut lines = text.lines();
        if lines.next()? != MAGIC {
            return None;
        }
        let root = parse_hex32(lines.next()?.strip_prefix("root=")?)?;
        let offset = lines.next()?.strip_prefix("offset=")?.parse().ok()?;
        Some(Self { root, offset })
    }
}

fn hex_digit(nibble: u8) -> char {
    char::from_digit(u32::from(nibble), 16).expect("nibble is < 16")
}

fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 || !s.is_ascii() {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = [0u8; 32];
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = (bytes[2 * i] as char).to_digit(16)?;
        let lo = (bytes[2 * i + 1] as char).to_digit(16)?;
        *slot = u8::try_from(hi * 16 + lo).expect("two hex digits fit a byte");
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> ResumeState {
        let mut root = [0u8; 32];
        root[0] = 0x84;
        root[1] = 0x2e;
        root[31] = 0x9a;
        ResumeState {
            root,
            offset: 123_456_789,
        }
    }

    #[test]
    fn round_trips() {
        let s = state();
        assert_eq!(ResumeState::parse(&s.render()), Some(s));
    }

    #[test]
    fn render_is_stable() {
        let text = state().render();
        assert!(text.starts_with("directswarm-resume v1\nroot=842e"));
        assert!(text.ends_with("offset=123456789\n"));
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(ResumeState::parse(""), None);
        assert_eq!(ResumeState::parse("directswarm-resume v2\nroot=00\n"), None);
        let mut text = state().render();
        text = text.replace("root=84", "root=zz");
        assert_eq!(ResumeState::parse(&text), None);
    }

    #[test]
    fn rejects_bad_offset() {
        let text = state().render().replace("offset=123456789", "offset=-1");
        assert_eq!(ResumeState::parse(&text), None);
    }
}
