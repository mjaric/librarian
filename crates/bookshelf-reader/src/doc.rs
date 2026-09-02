//! The document model a [`Reader`](crate::Reader) hands back: sections in
//! reading order, block boundaries as char ranges, and the wire-ish types
//! the UI navigates by.

use ropey::Rope;

/// Which mirrored container the bytes came in — mirrors the provider's
/// `Format` keys (`txt`, `html.zip`, `epub.images`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFormat {
    Txt,
    HtmlZip,
    EpubImages,
}

impl SourceFormat {
    /// Parse a wire format key as spelled by `FileOffer.format`.
    pub fn parse_key(s: &str) -> Option<Self> {
        match s {
            "txt" => Some(SourceFormat::Txt),
            "html.zip" => Some(SourceFormat::HtmlZip),
            "epub.images" => Some(SourceFormat::EpubImages),
            _ => None,
        }
    }
}

/// One reading section: a single txt document, the html doc inside a
/// `-h.zip`, or one spine item of an EPUB.
#[derive(Debug, Clone)]
pub struct Section {
    /// Stable id: `txt:0` | `zip:{entry_name}` | `spine:{index}`.
    pub id: String,
    pub title: Option<String>,
    /// Sanitized HTML fragment (body content, no `<body>` wrapper).
    /// Empty for txt sections — they render from `blocks` alone.
    pub html: String,
    /// Normalized text of this section (see `normalize`); char-indexed.
    pub text: Rope,
    /// One span per block-level element, document order, non-overlapping
    /// char ranges into `text` covering each block's own (direct) text.
    /// Empty spans (`start == end`) stand for childless block elements so
    /// positions stay index-aligned with the rendered DOM.
    pub blocks: Vec<BlockSpan>,
    /// Source document path inside the container: the resolved manifest
    /// href of the spine item (EPUB) or the zip member name of the main
    /// document (html.zip); `None` for txt, whose documents carry no
    /// links. What [`Reader::section_index_for_path`](crate::Reader)
    /// matches in-book link hrefs against.
    pub source: Option<String>,
}

/// Char range `[start, end)` into the owning section's `text`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockSpan {
    pub start: usize,
    pub end: usize,
}

/// One table-of-contents row (EPUB only).
#[derive(Debug, Clone)]
pub struct TocEntry {
    pub title: Option<String>,
    pub section_id: String,
}

#[cfg(test)]
mod tests {
    use super::SourceFormat;

    #[test]
    fn wire_keys_round_trip() {
        assert_eq!(SourceFormat::parse_key("txt"), Some(SourceFormat::Txt));
        assert_eq!(
            SourceFormat::parse_key("html.zip"),
            Some(SourceFormat::HtmlZip)
        );
        assert_eq!(
            SourceFormat::parse_key("epub.images"),
            Some(SourceFormat::EpubImages)
        );
        assert_eq!(SourceFormat::parse_key("cover"), None);
        assert_eq!(SourceFormat::parse_key(""), None);
    }
}
