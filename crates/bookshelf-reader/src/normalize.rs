//! The one normalization pipeline — a single pure function used for BOTH
//! section text and anchor fingerprints, so offsets recorded by the UI
//! always index text that was produced the same way.
//!
//! Pipeline: UTF-8 is decoded by the caller (`from_utf8_lossy`), then
//! CRLF/lone CR collapse to LF, control characters are dropped, the string
//! is NFKC-normalized (composed diacritics, ligatures, fullwidth forms),
//! and every whitespace run collapses to a single space. Newlines do not
//! survive: paragraph structure lives in `Section.blocks` (one char range
//! per block element), which the UI renders as separate elements — the
//! text itself is one continuous normalized line.

use unicode_normalization::UnicodeNormalization;
/// NFKC is applied to the whole string, never per char: normalization is
/// context-sensitive across char boundaries (`e` + combining cedilla),
/// so per-char mapping would leave decomposed pairs behind.
pub fn normalize(input: &str) -> String {
    // Mac-era lone CRs behave exactly like newlines from here on.
    let lf = input.replace("\r\n", "\n").replace('\r', "\n");
    let nfkc: String = lf.nfkc().collect();

    let mut out = String::with_capacity(nfkc.len());
    let mut in_space = false;
    for c in nfkc.chars() {
        if c.is_whitespace() {
            in_space = true; // emitted only if visible text follows
        } else if c.is_control() {
            // dropped outright: zero-width controls have no reading voice
        } else {
            if in_space && !out.is_empty() {
                out.push(' ');
            }
            in_space = false;
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::normalize;

    #[test]
    fn crlf_and_lone_cr_become_spaces() {
        assert_eq!(normalize("a\r\nb"), "a b");
        assert_eq!(normalize("a\rb"), "a b");
        assert_eq!(normalize("a\n\n\nb"), "a b");
    }

    #[test]
    fn nfkc_composes_decomposed_diacritics() {
        // e + combining acute → precomposed é
        assert_eq!(normalize("e\u{301}galite"), "\u{e9}galite");
        // already-composed input is untouched
        assert_eq!(normalize("\u{e9}galit\u{e9}"), "\u{e9}galit\u{e9}");
        // compatibility: ligature and fullwidth forms fold to ASCII
        assert_eq!(normalize("\u{fb01}ne"), "fine");
        assert_eq!(normalize("\u{ff21}"), "A");
    }

    #[test]
    fn whitespace_runs_collapse_and_edges_trim() {
        assert_eq!(normalize("a \t\n  b"), "a b");
        assert_eq!(normalize("  padded  "), "padded");
        assert_eq!(normalize("a\u{a0}b"), "a b"); // NBSP is whitespace
    }

    #[test]
    fn control_chars_are_dropped() {
        assert_eq!(normalize("a\u{0}b\u{7}c\u{1b}d"), "abcd");
        // ...but the text content itself survives
        assert_eq!(normalize("\u{9}word\u{a}"), "word");
    }

    #[test]
    fn empty_input_stays_empty() {
        assert_eq!(normalize(""), "");
        assert_eq!(normalize("\r\n\r\n  \u{0}"), "");
    }
}
