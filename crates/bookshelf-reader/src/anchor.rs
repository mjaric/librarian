//! Reading-position anchors: where the eye stopped, stored as text —
//! never scrollTop, which dies with every re-layout.
//!
//! An [`AnchorRecord`] names a section (`doc`), a char offset into its
//! normalized text (`start`), and a fingerprint [`Quote`] (±16 chars of
//! context around ~32 chars of exact text). Restoring tries the
//! fingerprint first — so a re-render, re-wrap or mild edit upstream of
//! the position still lands the reader where they were — and falls back
//! to the plain clamped offset, then to `None` (caller shows the section
//! start). Anchoring is deliberately per-section only: positions never
//! cross section boundaries.

use serde::{Deserialize, Serialize};

use crate::Reader;
use crate::doc::Section;

/// `scheme` value of every record this module writes.
pub const SCHEME: &str = "bookshelf-anchor-v1";

/// Length of the exact fingerprint text.
const EXACT_CHARS: usize = 32;
/// Context captured on each side of `exact`.
const SIDE_CHARS: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnchorRecord {
    pub scheme: String,
    /// Section id (`txt:0` | `zip:{entry}` | `spine:{index}`).
    pub doc: String,
    /// Char offset into the section's normalized text.
    pub start: usize,
    pub quote: Quote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quote {
    pub exact: String,
    pub prefix: String,
    pub suffix: String,
}

/// Capture the anchor at `char_offset` (clamped into the section).
pub fn anchor_at(section: &Section, char_offset: usize) -> AnchorRecord {
    let len = section.text.len_chars();
    let start = char_offset.min(len);
    let exact_end = (start + EXACT_CHARS).min(len);
    AnchorRecord {
        scheme: SCHEME.into(),
        doc: section.id.clone(),
        start,
        quote: Quote {
            exact: rope_chars(&section.text, start, exact_end),
            prefix: rope_chars(&section.text, start.saturating_sub(SIDE_CHARS), start),
            suffix: rope_chars(&section.text, exact_end, exact_end + SIDE_CHARS),
        },
    }
}

/// Ladder: (1) fingerprint match against the record's section — strongest
/// quote first, occurrence nearest the recorded offset wins; (2) plain
/// clamped offset; `None` only when the section no longer exists, so the
/// caller can fall back (e.g. proportionally).
pub fn resolve(reader: &Reader, record: &AnchorRecord) -> Option<(String, usize)> {
    let section = reader.sections().iter().find(|s| s.id == record.doc)?;
    let len = section.text.len_chars();
    let start = record.start.min(len);

    if !record.quote.exact.is_empty() {
        let q = &record.quote;
        let with_suffix = format!("{}{}", q.exact, q.suffix);
        let with_prefix = format!("{}{}", q.prefix, q.exact);
        let full = format!("{}{}{}", q.prefix, q.exact, q.suffix);
        let prefix_chars = q.prefix.chars().count();
        // (haystack, chars to skip past to reach the anchor within a hit)
        let ladders = [
            (full.as_str(), prefix_chars),
            (with_suffix.as_str(), 0),
            (with_prefix.as_str(), prefix_chars),
            (q.exact.as_str(), 0),
        ];
        // One owned copy of the section text per resolve; occurrence search
        // is then plain `str::find` with char-offset bookkeeping.
        let hay = section.text.to_string();
        for (needle, lead) in ladders {
            if let Some(pos) = nearest_occurrence(&hay, needle, start) {
                return Some((record.doc.clone(), (pos + lead).min(len)));
            }
        }
    }

    Some((record.doc.clone(), start))
}

/// Char offset of the `needle` occurrence closest to `near`.
fn nearest_occurrence(hay: &str, needle: &str, near: usize) -> Option<usize> {
    let mut best: Option<(usize, usize)> = None; // (distance, char offset)
    let mut from = 0usize;
    // Step one char past each hit, not needle.len(): repeated text (the
    // "echo echo" case) overlaps, and skipping candidates here would
    // silently pick a farther occurrence.
    while let Some(byte_hit) = hay[from..].find(needle) {
        let byte_pos = from + byte_hit;
        let char_pos = hay[..byte_pos].chars().count();
        let dist = char_pos.abs_diff(near);
        if best.is_none_or(|(bd, _)| dist < bd) {
            best = Some((dist, char_pos));
        }
        from = byte_pos + hay[byte_pos..].chars().next().map_or(1, char::len_utf8);
    }
    best.map(|(_, pos)| pos)
}

fn rope_chars(text: &ropey::Rope, start: usize, end: usize) -> String {
    let end = end.min(text.len_chars());
    text.get_slice(start..end)
        .map(|s| s.to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{anchor_at, resolve};
    use crate::Reader;
    use crate::doc::{BlockSpan, Section};

    fn section_of(id: &str, text: &str) -> Section {
        Section {
            id: id.into(),
            title: None,
            html: String::new(),
            text: ropey::Rope::from_str(text),
            blocks: vec![BlockSpan {
                start: 0,
                end: text.chars().count(),
            }],
            source: None,
        }
    }

    fn reader_with(sections: Vec<Section>) -> Reader {
        Reader::from_parts(
            crate::doc::SourceFormat::Txt,
            sections,
            Vec::new(),
            Default::default(),
        )
    }

    #[test]
    fn round_trip_restores_the_offset() {
        let text = "It is a truth universally acknowledged, that a single man in \
                    possession of a good fortune, must be in want of a wife.";
        let s = section_of("txt:0", text);
        let r = reader_with(vec![s.clone()]);
        for at in [0usize, 7, 40, text.chars().count()] {
            let rec = anchor_at(&s, at);
            assert_eq!(rec.scheme, "bookshelf-anchor-v1");
            assert_eq!(rec.doc, "txt:0");
            assert_eq!(resolve(&r, &rec), Some(("txt:0".into(), at)));
        }
    }

    #[test]
    fn quote_covers_context_window() {
        let s = section_of("txt:0", &"x".repeat(1000));
        let rec = anchor_at(&s, 500);
        assert_eq!(rec.quote.exact.chars().count(), 32);
        assert_eq!(rec.quote.prefix.chars().count(), 16);
        assert_eq!(rec.quote.suffix.chars().count(), 16);
    }

    #[test]
    fn offsets_clamp_at_section_bounds() {
        let s = section_of("txt:0", "short");
        let rec = anchor_at(&s, 10_000);
        assert_eq!(rec.start, 5);
        let r = reader_with(vec![s]);
        assert_eq!(resolve(&r, &rec), Some(("txt:0".into(), 5)));
    }

    #[test]
    fn empty_section_anchors_to_zero() {
        let s = section_of("txt:0", "");
        let rec = anchor_at(&s, 4);
        assert_eq!(rec.start, 0);
        assert!(rec.quote.exact.is_empty());
        let r = reader_with(vec![s]);
        assert_eq!(resolve(&r, &rec), Some(("txt:0".into(), 0)));
    }

    #[test]
    fn fingerprint_survives_edits_elsewhere() {
        let text = "the quick brown fox jumps over the lazy dog again and again";
        let s = section_of("txt:0", text);
        let rec = anchor_at(&s, 30);
        // Edit the opening of the document, far from the anchor.
        let mutated = format!("DRACULA {text}");
        let r = reader_with(vec![section_of("txt:0", &mutated)]);
        let (doc, off) = resolve(&r, &rec).expect("still resolves");
        assert_eq!(doc, "txt:0");
        assert_eq!(off, 38); // shifted by the inserted prefix
    }

    #[test]
    fn fingerprint_survives_edits_around_the_offset() {
        let text = &"word ".repeat(200);
        let s = section_of("txt:0", text);
        let rec = anchor_at(&s, 300);
        // Corrupt the recorded prefix/suffix context but keep `exact`.
        let mut broken = rec.clone();
        broken.quote.prefix = "zzz".into();
        broken.quote.suffix = "zzz".into();
        let r = reader_with(vec![section_of("txt:0", text)]);
        assert_eq!(resolve(&r, &broken), Some(("txt:0".into(), 300)));
    }

    #[test]
    fn heavy_edits_fall_back_to_clamped_offset() {
        let s = section_of("txt:0", &"lorem ipsum dolor ".repeat(50));
        let rec = anchor_at(&s, 200);
        // Replace the whole text: fingerprint cannot match anywhere.
        let r = reader_with(vec![section_of("txt:0", "entirely different words here")]);
        assert_eq!(resolve(&r, &rec), Some(("txt:0".into(), 29)));
    }

    #[test]
    fn unknown_doc_resolves_to_none() {
        let s = section_of("spine:0", "text");
        let mut rec = anchor_at(&s, 2);
        rec.doc = "spine:9".into();
        let r = reader_with(vec![s]);
        assert_eq!(resolve(&r, &rec), None);
    }
}
