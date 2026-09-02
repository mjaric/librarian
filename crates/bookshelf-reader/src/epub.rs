//! EPUB opener for `pg{id}-images.epub`: a zip of XHTML + images with
//! OCF/OPF/NCX metadata, pull-parsed with quick-xml in the same style as
//! `gutenberg_org/rdf.rs` (namespace-agnostic local-name matching).
//!
//! Spine order defines sections (`spine:{index}`); the TOC comes from the
//! EPUB3 nav document when the manifest advertises one (`properties` with
//! `nav`), falling back to the EPUB2 NCX. Manifest images/fonts become
//! assets keyed by their OPF-root-relative path.

use std::io::Read;

use quick_xml::NsReader;
use quick_xml::events::Event;

use crate::doc::{Section, SourceFormat, TocEntry};
use crate::{Reader, dom};

/// `META-INF/container.xml` → the rootfile's `full-path` (the OPF).
fn parse_container(xml: &str) -> anyhow::Result<String> {
    let mut reader = NsReader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    loop {
        let event = reader.read_event_into(&mut buf)?;
        match &event {
            Event::Start(e) | Event::Empty(e) if e.name().local_name().as_ref() == b"rootfile" => {
                if let Some(path) = attr_local(e, b"full-path") {
                    return Ok(path);
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    anyhow::bail!("no rootfile in META-INF/container.xml")
}

#[derive(Debug, Clone)]
struct OpfItem {
    id: String,
    href: String,
    media_type: String,
    properties: Option<String>,
}

#[derive(Debug, Default)]
struct Opf {
    items: Vec<OpfItem>,
    /// Spine idrefs, in reading order.
    spine: Vec<String>,
    /// `spine@toc` — the NCX manifest id (EPUB2).
    toc: Option<String>,
}

/// Pull-parse the OPF: manifest items + spine order.
fn parse_opf(xml: &str) -> anyhow::Result<Opf> {
    let mut reader = NsReader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut opf = Opf::default();
    loop {
        let event = reader.read_event_into(&mut buf)?;
        match &event {
            Event::Start(e) | Event::Empty(e) => match e.name().local_name().as_ref() {
                b"item" => {
                    let item = OpfItem {
                        id: attr_local(e, b"id").unwrap_or_default(),
                        href: attr_local(e, b"href").unwrap_or_default(),
                        media_type: attr_local(e, b"media-type").unwrap_or_default(),
                        properties: attr_local(e, b"properties"),
                    };
                    if !item.id.is_empty() && !item.href.is_empty() {
                        opf.items.push(item);
                    }
                }
                b"spine" => {
                    if opf.toc.is_none() {
                        opf.toc = attr_local(e, b"toc");
                    }
                }
                b"itemref" => {
                    if let Some(idref) = attr_local(e, b"idref") {
                        opf.spine.push(idref);
                    }
                }
                _ => {}
            },
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    if opf.items.is_empty() {
        anyhow::bail!("OPF holds no manifest items");
    }
    Ok(opf)
}

/// EPUB3 nav document → (raw href, title) pairs, document order, nested
/// lists flattened.
fn toc_from_nav(nav_xml: &str) -> Vec<(String, String)> {
    let dom = scraper::Html::parse_document(nav_xml);
    let nav_sel = scraper::Selector::parse("nav").expect("static selector");
    let a_sel = scraper::Selector::parse("a[href]").expect("static selector");
    // The toc-labeled nav wins; otherwise every nav contributes.
    let toc_navs: Vec<_> = dom
        .select(&nav_sel)
        .filter(|n| n.attr("epub:type") == Some("toc"))
        .collect();
    let navs: Vec<_> = if toc_navs.is_empty() {
        dom.select(&nav_sel).collect()
    } else {
        toc_navs
    };
    let mut out = Vec::new();
    for nav in navs {
        for a in nav.select(&a_sel) {
            let href = a.attr("href").unwrap_or_default();
            let title = crate::normalize::normalize(&a.text().collect::<String>());
            if !href.is_empty() && !title.is_empty() {
                out.push((href.to_string(), title));
            }
        }
    }
    out
}

/// EPUB2 NCX fallback → (raw href, title) pairs, document order.
fn toc_from_ncx(xml: &str) -> Vec<(String, String)> {
    let mut reader = NsReader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut out = Vec::new();
    let mut src: Option<String> = None;
    let mut in_text = false;
    let mut text = String::new();
    loop {
        let event = match reader.read_event_into(&mut buf) {
            Ok(ev) => ev,
            Err(_) => break, // a truncated NCX still yields what it had
        };
        match &event {
            Event::Start(e) | Event::Empty(e) => match e.name().local_name().as_ref() {
                b"content" => src = attr_local(e, b"src"),
                b"text" => {
                    in_text = true;
                    text.clear();
                }
                _ => {}
            },
            Event::Text(t) => {
                if in_text {
                    if let Ok(raw) = t.decode() {
                        match quick_xml::escape::unescape(&raw) {
                            Ok(u) => text.push_str(&u),
                            Err(_) => text.push_str(&raw),
                        }
                    }
                }
            }
            Event::End(e) => match e.name().local_name().as_ref() {
                b"text" => in_text = false,
                b"navPoint" => {
                    if let Some(src) = src.take() {
                        let title = crate::normalize::normalize(&text);
                        if !title.is_empty() {
                            out.push((src, title));
                        }
                    }
                    text.clear();
                }
                _ => {}
            },
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    out
}

/// Join a document-relative `href` onto `base_dir`, collapsing `.`/`..`
/// segments. Percent-encoding is left alone: Gutenberg hrefs are plain.
/// Also the join half of [`crate::Reader::section_index_for_path`].
pub(crate) fn resolve_href(base_dir: &str, href: &str) -> String {
    let mut parts: Vec<&str> = base_dir
        .split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect();
    for seg in href.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    parts.join("/")
}

/// Directory part of an OPF-root-relative path ("" when at the root).
pub(crate) fn dir_of(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[..i],
        None => "",
    }
}

/// `href` without its `#fragment` — the document a link points at.
pub(crate) fn strip_fragment(href: &str) -> &str {
    match href.find('#') {
        Some(i) => &href[..i],
        None => href,
    }
}

fn is_document(media_type: &str) -> bool {
    matches!(media_type, "application/xhtml+xml" | "text/html")
}

fn is_asset(media_type: &str) -> bool {
    media_type.starts_with("image/")
        || media_type.starts_with("font/")
        || media_type.starts_with("application/font")
        || media_type.starts_with("application/x-font")
}

/// Attribute lookup by local name (works whatever the prefix is) — same
/// shape as `gutenberg_org/rdf.rs`.
fn attr_local(e: &quick_xml::events::BytesStart<'_>, want: &[u8]) -> Option<String> {
    e.attributes()
        .flatten()
        .find(|a| {
            let key = a.key.as_ref();
            let local = key.rsplit(|&b| b == b':').next().unwrap_or(key);
            local == want
        })
        .and_then(|a| {
            let raw = String::from_utf8_lossy(a.value.as_ref());
            quick_xml::escape::unescape(&raw)
                .ok()
                .map(|v| v.into_owned())
        })
}

pub fn open_epub(bytes: &[u8]) -> anyhow::Result<Reader> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))
        .map_err(|e| anyhow::anyhow!("cannot read epub container: {e}"))?;

    let container = read_member(&mut archive, "META-INF/container.xml")?;
    let opf_path = parse_container(&String::from_utf8_lossy(&container))?;
    let opf_dir = dir_of(&opf_path);
    let opf_xml = read_member(&mut archive, &opf_path)?;
    let opf = parse_opf(&String::from_utf8_lossy(&opf_xml))?;

    let item_by_id = |id: &str| opf.items.iter().find(|i| i.id == id);

    // --- TOC first, so section titles can use it -----------------------
    // Entries become (OPF-root-relative path, title).
    let nav_item = opf.items.iter().find(|i| {
        i.properties
            .as_deref()
            .is_some_and(|p| p.split_whitespace().any(|t| t == "nav"))
    });
    let ncx_id = opf.toc.clone().or_else(|| {
        opf.items
            .iter()
            .find(|i| i.media_type == "application/x-dtbncx+xml")
            .map(|i| i.id.clone())
    });
    let mut toc_titles: Vec<(String, String)> = Vec::new();
    if let Some(nav) = nav_item {
        let nav_path = resolve_href(opf_dir, &nav.href);
        let nav_dir = dir_of(&nav_path).to_string();
        if let Ok(nav_xml) = read_member(&mut archive, &nav_path) {
            toc_titles = toc_from_nav(&String::from_utf8_lossy(&nav_xml))
                .into_iter()
                .map(|(href, title)| (resolve_href(&nav_dir, strip_fragment(&href)), title))
                .collect();
        }
    } else if let Some(id) = ncx_id.as_deref().and_then(|id| item_by_id(id)) {
        let ncx_path = resolve_href(opf_dir, &id.href);
        let ncx_dir = dir_of(&ncx_path).to_string();
        if let Ok(ncx_xml) = read_member(&mut archive, &ncx_path) {
            toc_titles = toc_from_ncx(&String::from_utf8_lossy(&ncx_xml))
                .into_iter()
                .map(|(href, title)| (resolve_href(&ncx_dir, strip_fragment(&href)), title))
                .collect();
        }
    }

    // --- sections in spine order ---------------------------------------
    let mut sections = Vec::new();
    let mut toc = Vec::new();
    for (index, idref) in opf.spine.iter().enumerate() {
        let Some(item) = item_by_id(idref) else {
            anyhow::bail!("spine item {idref} not present in the OPF manifest");
        };
        if !is_document(&item.media_type) {
            continue;
        }
        let path = resolve_href(opf_dir, &item.href);
        let raw = read_member(&mut archive, &path)
            .map_err(|e| anyhow::anyhow!("spine item {idref} ({path}) unreadable: {e}"))?;
        let doc = String::from_utf8_lossy(&raw);
        let title = toc_titles
            .iter()
            .find(|(p, _)| p == &path)
            .map(|(_, t)| t.clone())
            .or_else(|| crate::htmlzip::extract_title(&doc));
        // Sanitize the body content only — a whole-document clean would
        // leak <title> text (ammonia unwraps head elements).
        let body = dom::body_inner_html(&doc);
        let sanitized = dom::sanitize(&body);
        let walked = dom::walk_fragment(&sanitized);
        sections.push(Section {
            id: format!("spine:{index}"),
            title,
            html: sanitized,
            text: ropey::Rope::from_str(&walked.text),
            blocks: walked.blocks,
            source: Some(path.clone()),
        });

        // TOC rows pointing at this document, in TOC order.
        for (target, title) in &toc_titles {
            if *target == path {
                toc.push(TocEntry {
                    title: Some(title.clone()),
                    section_id: format!("spine:{index}"),
                });
            }
        }
    }

    // --- images and fonts as assets ------------------------------------
    let mut assets = std::collections::BTreeMap::new();
    for item in &opf.items {
        if !is_asset(&item.media_type) {
            continue;
        }
        let path = resolve_href(opf_dir, &item.href);
        if let Ok(bytes) = read_member(&mut archive, &path) {
            assets.insert(path, bytes);
        }
    }

    Ok(Reader::from_parts(
        SourceFormat::EpubImages,
        sections,
        toc,
        assets,
    ))
}

fn read_member(
    archive: &mut zip::ZipArchive<std::io::Cursor<&[u8]>>,
    name: &str,
) -> anyhow::Result<Vec<u8>> {
    let mut file = archive
        .by_name(name)
        .map_err(|e| anyhow::anyhow!("member {name} missing from the epub: {e}"))?;
    let mut out = Vec::new();
    file.read_to_end(&mut out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const CONTAINER: &str = r#"<?xml version="1.0"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles><rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles>
</container>"#;

    const CH1: &str = r#"<html xmlns="http://www.w3.org/1999/xhtml"><head><title>Chapter One</title></head>
<body><h1>I.</h1><p>The ship &amp; the sea.</p><p><a href="ch2.xhtml#top">Next</a></p><script>bad()</script></body></html>"#;

    const CH2: &str = r#"<html xmlns="http://www.w3.org/1999/xhtml"><head></head>
<body><h1 id="top">II.</h1><p>Second chapter text.</p><p><a href="../text/ch1.xhtml">Back</a></p><img src="../images/pic.png" alt="map"/></body></html>"#;

    const PIC: &[u8] = b"\x89PNG-fake";

    fn opf(toc_marker: &str) -> String {
        // `toc_marker` is either `properties="nav"` on the nav item (EPUB3)
        // or `toc="ncx"` on the spine (EPUB2).
        format!(
            r#"<?xml version="1.0"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="uid">
  <manifest>
    <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" {toc_marker}/>
    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
    <item id="ch1" href="text/ch1.xhtml" media-type="application/xhtml+xml"/>
    <item id="ch2" href="text/ch2.xhtml" media-type="application/xhtml+xml"/>
    <item id="pic" href="images/pic.png" media-type="image/png"/>
  </manifest>
  <spine {spine_toc}>
    <itemref idref="ch1"/>
    <itemref idref="ch2"/>
  </spine>
</package>"#,
            toc_marker = toc_marker,
            spine_toc = if toc_marker.contains("nav") {
                ""
            } else {
                r#"toc="ncx""#
            },
        )
    }

    const NAV: &str = r#"<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<body><nav epub:type="toc"><ol>
  <li><a href="text/ch1.xhtml">The Voyage Out</a></li>
  <li><a href="text/ch2.xhtml">Home Waters</a></li>
</ol></nav></body></html>"#;

    const NCX: &str = r#"<?xml version="1.0"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <navMap>
    <navPoint id="n1"><navLabel><text>The Voyage Out</text></navLabel><content src="text/ch1.xhtml"/></navPoint>
    <navPoint id="n2"><navLabel><text>Home Waters</text></navLabel><content src="text/ch2.xhtml"/></navPoint>
  </navMap>
</ncx>"#;

    fn fixture_epub(nav: bool) -> Vec<u8> {
        let mut w = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        let mut add = |name: &str, data: &[u8]| {
            w.start_file(name, opts).unwrap();
            w.write_all(data).unwrap();
        };
        add("META-INF/container.xml", CONTAINER.as_bytes());
        add(
            "OEBPS/content.opf",
            opf(if nav { r#"properties="nav""# } else { "" }).as_bytes(),
        );
        if nav {
            add("OEBPS/nav.xhtml", NAV.as_bytes());
        } else {
            add("OEBPS/toc.ncx", NCX.as_bytes());
        }
        add("OEBPS/text/ch1.xhtml", CH1.as_bytes());
        add("OEBPS/text/ch2.xhtml", CH2.as_bytes());
        add("OEBPS/images/pic.png", PIC);
        w.finish().unwrap().into_inner()
    }

    #[test]
    fn spine_order_and_sections() {
        let r = open_epub(&fixture_epub(true)).unwrap();
        let ids: Vec<&str> = r.sections().iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, ["spine:0", "spine:1"]);
        let ch1 = &r.sections()[0];
        assert_eq!(ch1.text.to_string(), "I. The ship & the sea. Next");
        // Sanitization strips scripts from spine documents too.
        assert!(!ch1.html.contains("bad"));
    }

    #[test]
    fn sections_carry_source_paths_and_sanitized_targets() {
        let r = open_epub(&fixture_epub(true)).unwrap();
        let src: Vec<Option<&str>> = r.sections().iter().map(|s| s.source.as_deref()).collect();
        // Resolved manifest hrefs, OPF-root-relative.
        assert_eq!(
            src,
            [Some("OEBPS/text/ch1.xhtml"), Some("OEBPS/text/ch2.xhtml")]
        );
        // In-book link machinery survives sanitization: relative hrefs
        // pass through, `id` targets are kept.
        assert!(r.sections()[0].html.contains(r#"href="ch2.xhtml#top""#));
        assert!(r.sections()[1].html.contains(r#"id="top""#));
    }

    #[test]
    fn in_book_links_resolve_against_the_linking_document() {
        let r = open_epub(&fixture_epub(true)).unwrap();
        // Same directory, bare name, with a fragment.
        assert_eq!(
            r.section_index_for_path("OEBPS/text/ch1.xhtml", "ch2.xhtml#top"),
            Some(1)
        );
        // `./` and `../` segments collapse like a browser would join them.
        assert_eq!(
            r.section_index_for_path("OEBPS/text/ch1.xhtml", "./ch2.xhtml"),
            Some(1)
        );
        assert_eq!(
            r.section_index_for_path("OEBPS/text/ch2.xhtml", "../text/ch1.xhtml"),
            Some(0)
        );
        assert_eq!(
            r.section_index_for_path("OEBPS/text/ch2.xhtml", "../../OEBPS/text/ch2.xhtml"),
            Some(1)
        );
        // Never a section hop: fragment-only (same document), external
        // schemes, unknown documents.
        assert_eq!(
            r.section_index_for_path("OEBPS/text/ch1.xhtml", "#top"),
            None
        );
        assert_eq!(
            r.section_index_for_path("OEBPS/text/ch1.xhtml", "https://www.gutenberg.org/"),
            None
        );
        assert_eq!(
            r.section_index_for_path("OEBPS/text/ch1.xhtml", "mailto:x@example.org"),
            None
        );
        assert_eq!(
            r.section_index_for_path("OEBPS/text/ch1.xhtml", "missing.xhtml"),
            None
        );
        // With no usable origin the unique-suffix rule still finds the
        // document (mirrors `Reader::asset`).
        assert_eq!(r.section_index_for_path("", "ch2.xhtml"), Some(1));
    }

    #[test]
    fn toc_titles_from_epub3_nav() {
        let r = open_epub(&fixture_epub(true)).unwrap();
        let toc: Vec<(&str, &str)> = r
            .toc()
            .iter()
            .map(|t| (t.section_id.as_str(), t.title.as_deref().unwrap_or("")))
            .collect();
        assert_eq!(
            toc,
            [("spine:0", "The Voyage Out"), ("spine:1", "Home Waters")]
        );
        // TOC titles become section titles.
        assert_eq!(r.sections()[1].title.as_deref(), Some("Home Waters"));
    }

    #[test]
    fn toc_titles_from_ncx_fallback() {
        let r = open_epub(&fixture_epub(false)).unwrap();
        let titles: Vec<&str> = r
            .toc()
            .iter()
            .map(|t| t.title.as_deref().unwrap_or(""))
            .collect();
        assert_eq!(titles, ["The Voyage Out", "Home Waters"]);
    }

    #[test]
    fn asset_lookup_by_href_and_relative_src() {
        let r = open_epub(&fixture_epub(true)).unwrap();
        // Exact OPF-root-relative href.
        assert_eq!(r.asset("images/pic.png"), Some(PIC));
        // The document's relative `../images/pic.png` still resolves
        // uniquely via the suffix rule.
        assert_eq!(r.asset("pic.png"), Some(PIC));
        assert_eq!(r.asset("nope.png"), None);
    }

    #[test]
    fn missing_container_is_a_clear_error() {
        let mut w = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        w.start_file("nonsense.txt", zip::write::SimpleFileOptions::default())
            .unwrap();
        std::io::Write::write_all(&mut w, b"?").unwrap();
        let zip = w.finish().unwrap().into_inner();
        let err = match open_epub(&zip) {
            Err(e) => e,
            Ok(_) => panic!("expected a clear error for a missing container.xml"),
        };
        assert!(err.to_string().contains("container.xml"));
    }

    #[test]
    fn corrupt_input_errors_without_panicking() {
        assert!(open_epub(b"").is_err());
        assert!(open_epub(b"PK\x03\x04 garbage").is_err());
    }
}
#[cfg(test)]
mod reader_tests {
    // Reader-level invariants that span formats.

    #[test]
    fn total_chars_sums_across_sections() {
        let doc = b"Alpha.\n\nBeta.\n";
        let r = crate::Reader::open(crate::doc::SourceFormat::Txt, doc).unwrap();
        assert_eq!(r.total_chars(), r.sections()[0].text.len_chars());
        assert!(r.toc().is_empty()); // txt has no TOC
    }
}
