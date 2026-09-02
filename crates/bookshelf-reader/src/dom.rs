//! Sanitized-DOM → (text, blocks) extraction, shared by the html.zip and
//! EPUB openers. Callers sanitize FIRST (ammonia) and hand us the cleaned
//! fragment, so the walk sees exactly the tree the UI will render.
//!
//! Alignment invariant the UI relies on: one [`BlockSpan`] per block-level
//! element, in document order — including text-less blocks (empty spans).
//! `BLOCK_SELECTOR` is the mirror image of the walker's block set; a
//! `querySelectorAll(BLOCK_SELECTOR)` over the rendered section pairs up
//! with `Section.blocks` by index.
//!
//! A block's *direct* text (own text nodes and inline descendants) fills
//! its span; the first non-empty run wins, later runs of the same block
//! (text after a nested block) stay in `text` but keep no span — rare in
//! Gutenberg documents, and positions clamp into the nearest span.

use ego_tree::NodeRef;

use scraper::{Html, Node, Selector};

use crate::doc::BlockSpan;
use crate::normalize::normalize;

/// The `<body>`'s inner HTML. Sanitizing a whole document would leak
/// `<title>` text (ammonia unwraps head elements), so callers hand us
/// the body only; a document without one passes through unchanged.
pub(crate) fn body_inner_html(doc: &str) -> String {
    let dom = Html::parse_document(doc);
    let sel = Selector::parse("body").expect("static selector");
    match dom.select(&sel).next() {
        Some(body) => body.inner_html(),
        None => doc.to_string(),
    }
}

/// Selectors matching [`is_block`] — kept in lockstep by tests.
pub const BLOCK_SELECTOR: &str = "p,div,section,article,aside,nav,header,footer,main,\
figure,figcaption,blockquote,pre,h1,h2,h3,h4,h5,h6,ul,ol,li,dl,dt,dd,\
table,thead,tbody,tfoot,tr,td,th,caption,center";

fn is_block(tag: &str) -> bool {
    matches!(
        tag,
        "p" | "div"
            | "section"
            | "article"
            | "aside"
            | "nav"
            | "header"
            | "footer"
            | "main"
            | "figure"
            | "figcaption"
            | "blockquote"
            | "pre"
            | "h1"
            | "h2"
            | "h3"
            | "h4"
            | "h5"
            | "h6"
            | "ul"
            | "ol"
            | "li"
            | "dl"
            | "dt"
            | "dd"
            | "table"
            | "thead"
            | "tbody"
            | "tfoot"
            | "tr"
            | "td"
            | "th"
            | "caption"
            | "center"
    )
}

/// Elements whose entire subtree is metadata, never reading text.
fn is_skipped(tag: &str) -> bool {
    matches!(
        tag,
        "script" | "style" | "template" | "head" | "title" | "svg"
    )
}

pub(crate) struct Walked {
    pub text: String,
    pub blocks: Vec<BlockSpan>,
}

/// Walk a sanitized HTML fragment: normalized text plus one block span
/// per block-level element, document order.
pub(crate) fn walk_fragment(sanitized: &str) -> Walked {
    let mut walker = Walker {
        text: String::new(),
        blocks: Vec::new(),
        run: None,
        slots: Vec::new(),
    };
    let dom = Html::parse_fragment(sanitized);
    for child in dom.root_element().children() {
        walker.visit(child);
    }
    walker.close_run(); // loose trailing text; assigned only if a slot is open
    Walked {
        text: walker.text,
        blocks: walker.blocks,
    }
}

struct Walker {
    text: String,
    blocks: Vec<BlockSpan>,
    /// Raw text of the current inline run, normalized only when closed so
    /// NFKC never splits mid-word across element boundaries.
    run: Option<String>,
    /// Slot indices of open block elements (innermost last).
    slots: Vec<usize>,
}

impl Walker {
    fn visit(&mut self, node: NodeRef<'_, Node>) {
        match node.value() {
            Node::Text(t) => {
                // Text derefs to str; runs are closed before normalizing.
                self.run.get_or_insert_default().push_str(t);
            }
            Node::Element(el) => {
                let tag = el.name();
                if is_skipped(tag) {
                    return;
                }
                if tag == "br" {
                    self.run.get_or_insert_default().push(' ');
                    return;
                }
                if tag == "img" {
                    return; // images render via the UI; alt text stays out
                }
                if is_block(tag) {
                    if let Some(span) = self.close_run() {
                        self.assign(span); // run belongs to the enclosing block
                    }
                    let slot = self.blocks.len();
                    self.blocks.push(BlockSpan { start: 0, end: 0 });
                    self.slots.push(slot);
                    for child in node.children() {
                        self.visit(child);
                    }
                    if let Some(span) = self.close_run() {
                        self.assign(span); // this block's own direct text
                    }
                    self.slots.pop();
                } else {
                    // inline elements are transparent
                    for child in node.children() {
                        self.visit(child);
                    }
                }
            }
            _ => {} // comments, doctype, processing instructions
        }
    }

    /// Fill the innermost open slot — first non-empty run wins.
    fn assign(&mut self, span: BlockSpan) {
        if let Some(&slot) = self.slots.last() {
            let cur = &mut self.blocks[slot];
            if cur.start == cur.end {
                *cur = span;
            }
        }
    }

    fn close_run(&mut self) -> Option<BlockSpan> {
        let raw = self.run.take()?;
        let normalized = normalize(&raw);
        if normalized.is_empty() {
            return None;
        }
        if !self.text.is_empty() {
            self.text.push(' ');
        }
        let start = self.text.chars().count();
        self.text.push_str(&normalized);
        Some(BlockSpan {
            start,
            end: self.text.chars().count(),
        })
    }
}

/// The ammonia policy both openers share: the defaults (scripts, event
/// handlers and non-http(s)/mailto URLs stripped) plus the `id` attribute
/// on every allowed element, so the in-book fragment links the UI
/// intercepts still find their targets after sanitization. Relative
/// hrefs pass through (`UrlRelative::PassThrough` is the default) — the
/// UI claims them before they can reach the SPA router.
///
/// Clobbering caveat: an `id` shadows same-named properties on
/// `window`/`document` for page scripts. This is a local, read-only
/// reader, so the exposure is acceptable — but the obviously-clobbering
/// globals are dropped anyway.
pub(crate) fn sanitize(fragment: &str) -> String {
    ammonia::Builder::default()
        .add_generic_attributes(["id"])
        .attribute_filter(|_elem, attr, value| {
            if attr.eq_ignore_ascii_case("id")
                && matches!(
                    value.to_ascii_lowercase().as_str(),
                    "location" | "document" | "window"
                )
            {
                None // refuse the DOM-clobbering globals
            } else {
                Some(std::borrow::Cow::Borrowed(value))
            }
        })
        .clean(fragment)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_survive_sanitization() {
        let out = sanitize(r#"<h2 id="chap01">Contents</h2>"#);
        assert!(out.contains(r#"id="chap01""#), "id stripped: {out}");
    }

    #[test]
    fn clobbering_ids_are_dropped() {
        for name in ["location", "document", "window", "LOCATION"] {
            let out = sanitize(&format!(r#"<p id="{name}">x</p>"#));
            assert!(!out.contains("id="), "{name} survived: {out}");
        }
    }

    #[test]
    fn relative_hrefs_pass_and_scripts_still_die() {
        let out = sanitize(r#"<a href="text/ch2.xhtml#top">Next</a><script>bad()</script>"#);
        assert!(out.contains(r#"href="text/ch2.xhtml#top""#), "{out}");
        assert!(!out.contains("script") && !out.contains("bad"), "{out}");
    }
}
