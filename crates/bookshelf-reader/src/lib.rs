//! bookshelf-reader — in-browser book reading, decoupled from the server.
//!
//! Bytes in, document model out: `Reader::open` takes a mirrored file
//! (`pg{id}.txt`, `pg{id}-h.zip`, `pg{id}-images.epub`) and produces
//! sanitized sections with normalized text, block boundaries, a table of
//! contents and the container's images. No tokio/sqlx/reqwest/IO — the
//! crate compiles unchanged for native tests and the wasm UI bundle.
//!
//! Reading positions are anchored in *text*, never pixels: an
//! [`AnchorRecord`] names a section plus a char offset and carries a
//! fingerprint quote so the position survives re-layout and mild edits.
pub mod anchor;
mod doc;
mod dom;
mod epub;
mod htmlzip;
mod normalize;
mod txt;

use std::collections::BTreeMap;

pub use doc::{BlockSpan, Section, SourceFormat, TocEntry};

pub use anchor::{AnchorRecord, Quote, anchor_at, resolve};

/// Mirror of the walker's block set: `querySelectorAll` over this pairs
/// 1:1 (document order) with `Section.blocks` of a rendered section.
pub use dom::BLOCK_SELECTOR;

/// An opened book: parsed sections, optional TOC, and the container's
/// binary members (images, fonts) addressable by path.
pub struct Reader {
    format: SourceFormat,
    sections: Vec<Section>,
    toc: Vec<TocEntry>,
    assets: BTreeMap<String, Vec<u8>>,
}

impl Reader {
    /// Parse `bytes` according to the mirrored format. Errors are
    /// anyhow-contexted and never panic on corrupt input.
    pub fn open(format: SourceFormat, bytes: &[u8]) -> anyhow::Result<Reader> {
        let mut reader = match format {
            SourceFormat::Txt => txt::open_txt(bytes)?,
            SourceFormat::HtmlZip => htmlzip::open_html_zip(bytes)?,
            SourceFormat::EpubImages => epub::open_epub(bytes)?,
        };
        reader.format = format;
        Ok(reader)
    }

    /// Crate-internal assembly point for the three openers.
    pub(crate) fn from_parts(
        format: SourceFormat,
        sections: Vec<Section>,
        toc: Vec<TocEntry>,
        assets: BTreeMap<String, Vec<u8>>,
    ) -> Reader {
        Reader {
            format,
            sections,
            toc,
            assets,
        }
    }

    /// The parsed sections, in reading order.
    pub fn sections(&self) -> &[Section] {
        &self.sections
    }

    /// Table of contents (EPUB only; empty otherwise).
    pub fn toc(&self) -> &[TocEntry] {
        &self.toc
    }
    /// A binary member by path — EPUB manifest hrefs, or zip member names
    /// for the html.zip container. Lookup is exact first, then a unique
    /// `/path`-suffix match, so a document's relative `src` resolves even
    /// when the caller cannot reconstruct the item's base directory.
    pub fn asset(&self, path: &str) -> Option<&[u8]> {
        let path = path.trim_start_matches("./");
        if let Some(bytes) = self.assets.get(path) {
            return Some(bytes.as_slice());
        }
        let suffix = format!("/{path}");
        let mut hits = self.assets.iter().filter(|(k, _)| k.ends_with(&suffix));
        let first = hits.next()?;
        if hits.next().is_some() {
            return None; // ambiguous — refuse rather than guess
        }
        Some(first.1.as_slice())
    }

    /// The section an in-book link lands on. `href` is the link target
    /// as written in the book — a document path with an optional
    /// `#fragment`, relative to the linking document `from_path` (a
    /// [`Section::source`] path; `""` where sections have none). The
    /// join collapses `./` and `../` segments; the result matches the
    /// sections' source paths exactly first, then via a unique
    /// `/`-suffix, mirroring [`Reader::asset`]. External URLs,
    /// fragment-only hrefs (same document) and unknown documents return
    /// `None` — what those do is the UI's call.
    pub fn section_index_for_path(&self, from_path: &str, href: &str) -> Option<usize> {
        let bare = epub::strip_fragment(href);
        if bare.is_empty() {
            return None;
        }
        // A `:` in the first segment is a scheme (`https:`, `mailto:`,
        // `data:`), not a container path.
        if bare.split('/').next().is_some_and(|seg| seg.contains(':')) {
            return None;
        }
        let target = epub::resolve_href(epub::dir_of(from_path), bare);
        if let Some(i) = self
            .sections
            .iter()
            .position(|s| s.source.as_deref() == Some(target.as_str()))
        {
            return Some(i);
        }
        let suffix = format!("/{target}");
        let mut hits = self
            .sections
            .iter()
            .enumerate()
            .filter(|(_, s)| s.source.as_deref().is_some_and(|p| p.ends_with(&suffix)));
        let (i, _) = hits.next()?;
        hits.next().is_none().then_some(i)
    }

    /// Which container the bytes were parsed from.
    pub fn format(&self) -> SourceFormat {
        self.format
    }

    /// Sum of normalized section text, in chars — the denominator for
    /// proportional position fallbacks.
    pub fn total_chars(&self) -> usize {
        self.sections.iter().map(|s| s.text.len_chars()).sum()
    }
}

#[cfg(test)]
mod lookup_tests {
    use super::*;

    fn sec(id: &str, source: Option<&str>) -> Section {
        Section {
            id: id.into(),
            title: None,
            html: String::new(),
            text: ropey::Rope::from_str(""),
            blocks: Vec::new(),
            source: source.map(Into::into),
        }
    }

    #[test]
    fn ambiguous_suffix_targets_are_refused() {
        let r = Reader::from_parts(
            SourceFormat::EpubImages,
            vec![
                sec("a", Some("one/ch3.xhtml")),
                sec("b", Some("two/ch3.xhtml")),
            ],
            Vec::new(),
            BTreeMap::new(),
        );
        // Two candidates end in "/ch3.xhtml" — with no usable origin the
        // suffix rule refuses rather than guesses.
        assert_eq!(r.section_index_for_path("", "ch3.xhtml"), None);
        // A sibling directory is reached by escaping: from the document
        // one/ch3.xhtml, `two/ch3.xhtml` would mean one/two/… — the
        // exact target needs `../`.
        assert_eq!(
            r.section_index_for_path("one/ch3.xhtml", "../two/ch3.xhtml"),
            Some(1)
        );
        assert_eq!(r.section_index_for_path("", "two/ch3.xhtml"), Some(1));
    }
}
