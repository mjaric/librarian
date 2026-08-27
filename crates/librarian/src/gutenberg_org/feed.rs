//! Daily feed adapter for the official `today.rss` (sanctioned, ~2.7 KB,
//! regenerated nightly). RSS 0.91: channel `pubDate` + `<item>`/`<link>`
//! entries. Every listed id is returned — the feed includes updates of OLD
//! books, so each id is re-checked.

use quick_xml::events::Event;
use quick_xml::Reader;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedHead {
    /// Channel pubDate, verbatim (e.g. `Thu, 27 Aug 2026 17:51:19 -0400`).
    pub pub_date: String,
    /// Ebook ids in feed order.
    pub ids: Vec<i64>,
}

pub fn parse_feed(xml: &str) -> anyhow::Result<FeedHead> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    let mut head = FeedHead { pub_date: String::new(), ids: Vec::new() };
    let mut in_item = false;
    let mut in_pubdate = false;
    let mut text = String::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                let qname = e.name();
                let name = qname.as_ref();
                match name {
                    b"item" => in_item = true,
                    b"link" if in_item => {}
                    b"pubDate" if !in_item => in_pubdate = true,
                    _ => {}
                }
                text.clear();
            }
            Event::Text(t) => {
                if let Ok(raw) = t.decode() {
                    match quick_xml::escape::unescape(&raw) {
                        Ok(u) => text.push_str(&u),
                        Err(_) => text.push_str(&raw),
                    }
                }
            }
            Event::End(e) => {
                let qname = e.name();
                let name = qname.as_ref();
                match name {
                    b"item" => in_item = false,
                    b"link" => {
                        if in_item {
                            // https://www.gutenberg.org/ebooks/{id}
                            if let Some(id) = text.trim().rsplit('/').next() {
                                if let Ok(id) = id.parse::<i64>() {
                                    head.ids.push(id);
                                }
                            }
                        }
                    }
                    b"pubDate" => {
                        if in_pubdate {
                            in_pubdate = false;
                            head.pub_date = text.trim().to_string();
                        }
                    }
                    _ => {}
                }
                text.clear();
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    // Validate the date shape (RFC 2822) without keeping the parsed value.
    if !head.pub_date.is_empty() {
        time::OffsetDateTime::parse(&head.pub_date, &time::format_description::well_known::Rfc2822)
            .map_err(|e| anyhow::anyhow!("feed pubDate {:?} not RFC2822: {e}", head.pub_date))?;
    }
    Ok(head)
}
