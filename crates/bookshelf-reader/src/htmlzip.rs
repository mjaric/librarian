//! html.zip opener for `pg{id}-h.zip`: the container holds exactly one
//! main document (plus its images), so the largest `.html`/`.htm` member
//! wins. The document is sanitized with ammonia's default policy (scripts
//! and handlers stripped — intended), then walked for text and blocks.
//! Every other archive member is kept as an asset under its zip path, so
//! the UI can re-point `<img src>` at blob URLs.
//!
//! The zip stays a single in-memory archive — files stream whole from the
//! server, so the bytes are already here when we open them.

use std::io::Read;

use crate::doc::{Section, SourceFormat};
use crate::{Reader, dom};

pub fn open_html_zip(bytes: &[u8]) -> anyhow::Result<Reader> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))
        .map_err(|e| anyhow::anyhow!("cannot read zip container: {e}"))?;

    let doc_name = largest_html_member(&mut archive)?;
    let mut raw = Vec::new();
    archive
        .by_name(&doc_name)
        .map_err(|e| anyhow::anyhow!("cannot read member {doc_name}: {e}"))?
        .read_to_end(&mut raw)?;
    let doc = String::from_utf8_lossy(&raw);

    // <title> lives in <head>, which ammonia strips — grab it from the
    // raw document before sanitizing.
    let title = extract_title(&doc);
    // Sanitize the body content only — a whole-document clean would
    // leak <title> text (ammonia unwraps head elements).
    let body = dom::body_inner_html(&doc);
    let sanitized = dom::sanitize(&body);
    let walked = dom::walk_fragment(&sanitized);

    let mut assets = std::collections::BTreeMap::new();
    for i in 0..archive.len() {
        let Ok(mut file) = archive.by_index(i) else {
            continue; // a corrupt side member must not kill the document
        };
        if file.is_dir() || file.name() == doc_name {
            continue;
        }
        let mut member = Vec::new();
        if file.read_to_end(&mut member).is_ok() {
            assets.insert(file.name().to_string(), member);
        }
    }

    let section = Section {
        id: format!("zip:{doc_name}"),
        title,
        html: sanitized,
        text: ropey::Rope::from_str(&walked.text),
        blocks: walked.blocks,
        source: Some(doc_name.clone()),
    };
    Ok(Reader::from_parts(
        SourceFormat::HtmlZip,
        vec![section],
        Vec::new(),
        assets,
    ))
}

/// Name of the largest `.html`/`.htm` member, case-insensitive.
fn largest_html_member(
    archive: &mut zip::ZipArchive<std::io::Cursor<&[u8]>>,
) -> anyhow::Result<String> {
    let mut best: Option<(String, u64)> = None;
    for i in 0..archive.len() {
        let Ok(file) = archive.by_index(i) else {
            continue;
        };
        let name = file.name();
        let lower = name.to_ascii_lowercase();
        if !lower.ends_with(".html") && !lower.ends_with(".htm") {
            continue;
        }
        if best.as_ref().is_none_or(|(_, size)| file.size() > *size) {
            best = Some((name.to_string(), file.size()));
        }
    }
    best.map(|(name, _)| name)
        .ok_or_else(|| anyhow::anyhow!("zip holds no .html/.htm document"))
}

/// The `<title>` of the raw document, if any — entity-decoded via the
/// scraper parse, whitespace-collapsed by the shared normalizer.
pub(crate) fn extract_title(doc: &str) -> Option<String> {
    let dom = scraper::Html::parse_document(doc);
    let sel = scraper::Selector::parse("title").ok()?;
    let title: String = dom
        .select(&sel)
        .next()?
        .text()
        .collect::<Vec<_>>()
        .join(" ");
    let title = crate::normalize::normalize(&title);
    if title.is_empty() { None } else { Some(title) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const DOC: &str = r##"<html><head><title>Pride &amp; Prejudice</title>
<style>body { color: red }</style></head><body>
<h1>Chapter 1</h1>
<script>alert("evil")</script>
<p>It is a truth <b>universally</b> acknowledged.</p>
<div class="chapter"><p>Second paragraph.</p><p>Third.</p></div>
<img src="images/pic.jpg" alt="frontispiece">
<p>caf&eacute; &lt;escaped&gt;</p>
</body></html>"##;

    fn fixture_zip(html: &str, extra: &[(&str, &[u8])]) -> Vec<u8> {
        let mut w = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        for (name, data) in
            std::iter::once(("pg1342-h.html", html.as_bytes())).chain(extra.iter().copied())
        {
            w.start_file(name, opts).unwrap();
            w.write_all(data).unwrap();
        }
        w.finish().unwrap().into_inner()
    }

    #[test]
    fn sanitization_removes_script_and_style() {
        let r = open_html_zip(&fixture_zip(DOC, &[])).unwrap();
        let s = &r.sections()[0];
        assert!(!s.html.contains("script"));
        assert!(!s.html.contains("alert"));
        assert!(s.html.contains("universally"));
    }

    #[test]
    fn blocks_and_text_align_in_document_order() {
        let r = open_html_zip(&fixture_zip(DOC, &[])).unwrap();
        let s = &r.sections()[0];
        // One span per block element in the sanitized DOM — including the
        // childless container div (empty span).
        let dom = scraper::Html::parse_fragment(&s.html);
        let sel = scraper::Selector::parse(dom::BLOCK_SELECTOR).unwrap();
        assert_eq!(dom.select(&sel).count(), s.blocks.len());

        let spans: Vec<&crate::doc::BlockSpan> =
            s.blocks.iter().filter(|b| b.end > b.start).collect();
        // Block offsets are char offsets; collect rather than byte-slice.
        let rendered: Vec<String> = spans
            .iter()
            .map(|b| s.text.chars().skip(b.start).take(b.end - b.start).collect())
            .collect();
        assert_eq!(
            rendered,
            vec![
                "Chapter 1".to_string(),
                "It is a truth universally acknowledged.".to_string(),
                "Second paragraph.".to_string(),
                "Third.".to_string(),
                "café <escaped>".to_string()
            ]
        );
        // Spans are non-overlapping and ordered.
        for pair in spans.windows(2) {
            assert!(pair[0].end <= pair[1].start);
        }
    }

    #[test]
    fn entities_decode_into_text_and_title() {
        let r = open_html_zip(&fixture_zip(DOC, &[])).unwrap();
        let s = &r.sections()[0];
        assert_eq!(s.title.as_deref(), Some("Pride & Prejudice"));
        assert!(s.text.to_string().contains("café <escaped>"));
    }

    #[test]
    fn assets_resolve_by_name_and_suffix() {
        let zip = fixture_zip(DOC, &[("images/pic.jpg", b"\x89PNG-fake".as_slice())]);
        let r = open_html_zip(&zip).unwrap();
        assert_eq!(r.asset("images/pic.jpg"), Some(b"\x89PNG-fake".as_slice()));
        // Relative src without the directory still resolves uniquely.
        assert_eq!(r.asset("pic.jpg"), Some(b"\x89PNG-fake".as_slice()));
        assert_eq!(r.asset("missing.jpg"), None);
    }

    #[test]
    fn largest_html_member_wins() {
        let zip = fixture_zip(DOC, &[("tiny.html", b"<p>teaser</p>".as_slice())]);
        let r = open_html_zip(&zip).unwrap();
        assert_eq!(r.sections()[0].id, "zip:pg1342-h.html");
    }

    #[test]
    fn in_book_links_resolve_within_the_single_document() {
        let zip = fixture_zip(DOC, &[("images/pic.jpg", b"\x89PNG-fake".as_slice())]);
        let r = open_html_zip(&zip).unwrap();
        assert_eq!(r.sections()[0].source.as_deref(), Some("pg1342-h.html"));
        // The container holds one document: self-links (with or without
        // a fragment) land on it; other members are not sections.
        assert_eq!(
            r.section_index_for_path("pg1342-h.html", "./pg1342-h.html#chap1"),
            Some(0)
        );
        assert_eq!(
            r.section_index_for_path("pg1342-h.html", "pg1342-h.html"),
            Some(0)
        );
        assert_eq!(
            r.section_index_for_path("pg1342-h.html", "other.html"),
            None
        );
        assert_eq!(r.section_index_for_path("pg1342-h.html", "#chap1"), None);
    }

    #[test]
    fn zip_without_html_is_a_clear_error() {
        // Fixture with no `.html` member at all: build it directly.
        let mut w = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        w.start_file("only.txt", zip::write::SimpleFileOptions::default())
            .unwrap();
        std::io::Write::write_all(&mut w, b"bare").unwrap();
        let zip = w.finish().unwrap().into_inner();
        let err = match open_html_zip(&zip) {
            Err(e) => e,
            Ok(_) => panic!("expected a clear error for a zip without html"),
        };
        assert!(err.to_string().contains("no .html"));
    }

    #[test]
    fn corrupt_input_errors_without_panicking() {
        assert!(open_html_zip(b"not a zip at all").is_err());
        assert!(open_html_zip(b"").is_err());
    }
    #[test]
    fn ambiguous_suffix_lookup_is_refused() {
        let zip = fixture_zip(
            DOC,
            &[
                ("a/pic.jpg", b"one".as_slice()),
                ("b/pic.jpg", b"two".as_slice()),
            ],
        );
        let r = open_html_zip(&zip).unwrap();
        // Exact paths still win; a bare basename matching two members
        // must refuse rather than guess.
        assert_eq!(r.asset("a/pic.jpg"), Some(b"one".as_slice()));
        assert_eq!(r.asset("pic.jpg"), None);
    }

    #[test]
    fn empty_html_member_yields_empty_section() {
        let r = open_html_zip(&fixture_zip("", &[])).unwrap();
        let s = &r.sections()[0];
        assert_eq!(s.text.len_chars(), 0);
        assert_eq!(s.blocks.len(), 0);
    }
}
