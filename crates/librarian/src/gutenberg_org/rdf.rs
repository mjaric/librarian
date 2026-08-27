//! Project Gutenberg `pg{id}.rdf` parser (quick-xml pull parser matching
//! local element names within namespace URIs) plus the provider's `Format`
//! enum with its mirror-name mapping.
//!
//! Extracted per verified pg1342.rdf structure: publisher, issued, rights,
//! downloads, marc520 (description), marc908 (reading ease), creators
//! (name/birth/death/wikipedia), language, subjects (LCSH/LCC via
//! `dcam:memberOf` URI tail), type, bookshelves, and `hasFormat` entries
//! (URL + extent + modified) for our four formats.

use quick_xml::events::Event;
use quick_xml::name::ResolveResult;
use quick_xml::NsReader;
use serde::Serialize;

pub const NS_PGTERMS: &[u8] = b"http://www.gutenberg.org/2009/pgterms/";
pub const NS_DCTERMS: &[u8] = b"http://purl.org/dc/terms/";
pub const NS_RDF: &[u8] = b"http://www.w3.org/1999/02/22-rdf-syntax-ns#";
pub const NS_DCAM: &[u8] = b"http://purl.org/dc/dcam/";

/// The four mirrored formats. Mirror filenames differ from RDF pretty-URLs:
/// txt→`pg{id}.txt`, epub.images→`pg{id}-images.epub`, html.zip→`pg{id}-h.zip`,
/// cover→`pg{id}.cover.medium.jpg`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum Format {
    Txt,
    EpubImages,
    HtmlZip,
    Cover,
}

impl Format {
    pub const ALL: [Format; 4] = [Format::Txt, Format::EpubImages, Format::HtmlZip, Format::Cover];

    pub fn key(&self) -> &'static str {
        match self {
            Format::Txt => "txt",
            Format::EpubImages => "epub.images",
            Format::HtmlZip => "html.zip",
            Format::Cover => "cover",
        }
    }

    pub fn parse_key(s: &str) -> Option<Self> {
        match s {
            "txt" => Some(Format::Txt),
            "epub.images" => Some(Format::EpubImages),
            "html.zip" => Some(Format::HtmlZip),
            "cover" => Some(Format::Cover),
            _ => None,
        }
    }

    pub fn mirror_name(&self, id: i64) -> String {
        match self {
            Format::Txt => format!("pg{id}.txt"),
            Format::EpubImages => format!("pg{id}-images.epub"),
            Format::HtmlZip => format!("pg{id}-h.zip"),
            Format::Cover => format!("pg{id}.cover.medium.jpg"),
        }
    }

    /// Suffix of the RDF `hasFormat` pretty-URL for this format.
    fn rdf_suffix(&self) -> &'static str {
        match self {
            Format::Txt => ".txt.utf-8",
            Format::EpubImages => ".epub.images",
            Format::HtmlZip => "-h.zip",
            Format::Cover => ".cover.medium.jpg",
        }
    }

    pub fn matches_url(&self, url: &str) -> bool {
        url.ends_with(self.rdf_suffix())
    }
}

/// What an rsync itemize path can refer to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorEntry {
    Rdf,
    Format(Format),
}
pub fn parse_mirror_name(name: &str) -> Option<(i64, MirrorEntry)> {
    let file = name.rsplit('/').next()?;
    let rest = file.strip_prefix("pg")?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    let id: i64 = digits.parse().ok()?;
    let tail = &rest[digits.len()..];
    let kind = match tail {
        ".rdf" => MirrorEntry::Rdf,
        ".txt" => MirrorEntry::Format(Format::Txt),
        "-images.epub" => MirrorEntry::Format(Format::EpubImages),
        "-h.zip" => MirrorEntry::Format(Format::HtmlZip),
        ".cover.medium.jpg" => MirrorEntry::Format(Format::Cover),
        _ => return None,
    };
    Some((id, kind))
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Author {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub death: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wikipedia: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Subject {
    /// "LCSH" | "LCC" | "Other"
    pub scheme: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RdfFormat {
    pub format: Format,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extent: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct RdfBook {
    pub id: i64,
    pub r#type: String,
    pub title: String,
    pub language: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issued: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publisher: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rights: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reading_ease: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub downloads: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authors: Vec<Author>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subjects: Vec<Subject>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bookshelves: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub formats: Vec<RdfFormat>,
}

impl RdfBook {
    pub fn issued_date(&self) -> Option<time::Date> {
        self.issued
            .as_deref()
            .and_then(|s| time::Date::parse(s, &time::format_description::well_known::Iso8601::DEFAULT).ok())
    }

    pub fn format_entry(&self, f: Format) -> Option<&RdfFormat> {
        self.formats.iter().find(|e| e.format == f)
    }
}

#[derive(Default)]
struct Agent {
    name: Option<String>,
    birth: Option<i64>,
    death: Option<i64>,
    wikipedia: Option<String>,
}

#[derive(Clone, Copy, PartialEq)]
enum DescKind {
    Language,
    Subject,
    Type,
    Bookshelf,
}

#[derive(Clone, Copy, PartialEq)]
enum Capture {
    Publisher,
    Issued,
    Rights,
    Title,
    Downloads,
    Marc520,
    Marc908,
    RdfValue,
    Extent,
    Modified,
    AgentName,
    AgentBirth,
    AgentDeath,
}

/// One hasFormat accumulator. Later `/cache/epub/` URLs win over earlier
/// candidates for the same format (the generated cache matches the mirror).
#[derive(Default)]
struct FileAccum {
    url: Option<String>,
    extent: Option<i64>,
    modified: Option<String>,
}

/// Parse one `pg{id}.rdf` document.
pub fn parse_rdf(xml: &str) -> anyhow::Result<RdfBook> {
    let mut reader = NsReader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    let mut book = RdfBook::default();
    let mut in_ebook = false;
    let mut agent: Option<Agent> = None;
    let mut desc: Option<DescKind> = None;
    let mut file: Option<FileAccum> = None;
    let mut capture: Option<Capture> = None;
    let mut subject_scheme: Option<String> = None;
    let mut text = String::new();

    loop {
        let (ns, event) = reader.read_resolved_event_into(&mut buf)?;
        fn is(ns: &ResolveResult<'_>, event: &Event<'_>, uri: &[u8], local: &[u8]) -> bool {
            matches!(ns, ResolveResult::Bound(u) if u.as_ref() == uri)
                && reader_event_local(event, local)
        }
        match &event {
            Event::Start(e) | Event::Empty(e) => {
                let empty = matches!(event, Event::Empty(_));
                if is(&ns, &event, NS_PGTERMS, b"ebook") {
                    in_ebook = true;
                    if let Some(about) = attr_local(&e, b"about") {
                        if let Some(digits) = about.rsplit('/').next() {
                            book.id = digits.parse().unwrap_or(0);
                        }
                    }
                } else if !in_ebook {
                    // outside pgterms:ebook: nothing we care about
                } else if is(&ns, &event, NS_PGTERMS, b"agent") {
                    agent = Some(Agent::default());
                } else if is(&ns, &event, NS_DCTERMS, b"creator") {
                    // container; agent follows
                } else if is(&ns, &event, NS_DCTERMS, b"language") {
                    desc = Some(DescKind::Language);
                } else if is(&ns, &event, NS_DCTERMS, b"subject") {
                    desc = Some(DescKind::Subject);
                    subject_scheme = None;
                } else if is(&ns, &event, NS_DCTERMS, b"type") {
                    desc = Some(DescKind::Type);
                } else if is(&ns, &event, NS_PGTERMS, b"bookshelf") {
                    desc = Some(DescKind::Bookshelf);
                } else if is(&ns, &event, NS_DCTERMS, b"hasFormat") {
                    // container; pgterms:file follows
                } else if is(&ns, &event, NS_PGTERMS, b"file") {
                    file = Some(FileAccum {
                        url: attr_local(&e, b"about"),
                        extent: None,
                        modified: None,
                    });
                } else if is(&ns, &event, NS_DCAM, b"memberOf") {
                    if desc == Some(DescKind::Subject) {
                        if let Some(resource) = attr_local(&e, b"resource") {
                            let tail = resource.rsplit('/').next().unwrap_or("").to_string();
                            subject_scheme = Some(tail);
                        }
                    }
                } else if is(&ns, &event, NS_PGTERMS, b"webpage") {
                    if let (Some(a), Some(w)) = (&mut agent, attr_local(&e, b"resource")) {
                        a.wikipedia = Some(w);
                    }
                } else if is(&ns, &event, NS_DCTERMS, b"extent") && !empty {
                    capture = Some(Capture::Extent);
                } else if is(&ns, &event, NS_DCTERMS, b"modified") && !empty {
                    capture = Some(Capture::Modified);
                } else if is(&ns, &event, NS_DCTERMS, b"publisher") && !empty {
                    capture = Some(Capture::Publisher);
                } else if is(&ns, &event, NS_DCTERMS, b"issued") && !empty {
                    capture = Some(Capture::Issued);
                } else if is(&ns, &event, NS_DCTERMS, b"rights") && !empty {
                    capture = Some(Capture::Rights);
                } else if is(&ns, &event, NS_DCTERMS, b"title") && !empty {
                    capture = Some(Capture::Title);
                } else if is(&ns, &event, NS_PGTERMS, b"downloads") && !empty {
                    capture = Some(Capture::Downloads);
                } else if is(&ns, &event, NS_PGTERMS, b"marc520") && !empty {
                    capture = Some(Capture::Marc520);
                } else if is(&ns, &event, NS_PGTERMS, b"marc908") && !empty {
                    capture = Some(Capture::Marc908);
                } else if is(&ns, &event, NS_PGTERMS, b"name") && agent.is_some() && !empty {
                    capture = Some(Capture::AgentName);
                } else if is(&ns, &event, NS_PGTERMS, b"birthdate") && agent.is_some() && !empty {
                    capture = Some(Capture::AgentBirth);
                } else if is(&ns, &event, NS_PGTERMS, b"deathdate") && agent.is_some() && !empty {
                    capture = Some(Capture::AgentDeath);
                } else if is(&ns, &event, NS_RDF, b"value") && !empty {
                    capture = Some(Capture::RdfValue);
                }
                if matches!(event, Event::Start(_)) {
                    text.clear();
                }
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
                let local = e.name().local_name();
                let local = local.as_ref();
                if capture.is_some() {
                    let owned = text.trim().to_string();
                    let cap = capture.take();
                    match cap {
                        Some(Capture::Publisher) => book.publisher = Some(owned),
                        Some(Capture::Issued) => book.issued = Some(owned),
                        Some(Capture::Rights) => book.rights = Some(owned),
                        Some(Capture::Title) => book.title = owned,
                        Some(Capture::Downloads) => book.downloads = owned.parse().ok(),
                        Some(Capture::Marc520) => book.description = Some(owned),
                        Some(Capture::Marc908) => book.reading_ease = Some(owned),
                        Some(Capture::Extent) => {
                            if let Some(f) = file.as_mut() {
                                f.extent = owned.parse().ok();
                            }
                        }
                        Some(Capture::Modified) => {
                            if let Some(f) = file.as_mut() {
                                f.modified = Some(owned);
                            }
                        }
                        Some(Capture::AgentName) => {
                            if let Some(a) = agent.as_mut() {
                                a.name = Some(owned);
                            }
                        }
                        Some(Capture::AgentBirth) => {
                            if let Some(a) = agent.as_mut() {
                                a.birth = owned.parse().ok();
                            }
                        }
                        Some(Capture::AgentDeath) => {
                            if let Some(a) = agent.as_mut() {
                                a.death = owned.parse().ok();
                            }
                        }
                        Some(Capture::RdfValue) => match desc {
                            Some(DescKind::Language) => book.language = owned,
                            Some(DescKind::Subject) => {
                                let scheme = match subject_scheme.as_deref() {
                                    Some("LCSH") => "LCSH",
                                    Some("LCC") => "LCC",
                                    _ => "Other",
                                };
                                book.subjects.push(Subject {
                                    scheme: scheme.into(),
                                    value: owned,
                                });
                            }
                            Some(DescKind::Type) => book.r#type = owned,
                            Some(DescKind::Bookshelf) => book.bookshelves.push(owned),
                            None => {}
                        },
                        _ => {}
                    }
                }
                // closing containers
                if ns_matches(&ns, NS_PGTERMS) && local == b"agent" {
                    if let Some(a) = agent.take() {
                        if let Some(name) = a.name {
                            book.authors.push(Author {
                                name,
                                birth: a.birth,
                                death: a.death,
                                wikipedia: a.wikipedia,
                            });
                        }
                    }
                } else if ns_matches(&ns, NS_DCTERMS)
                    && matches!(local, b"language" | b"subject" | b"type")
                    || ns_matches(&ns, NS_PGTERMS) && local == b"bookshelf"
                {
                    desc = None;
                } else if ns_matches(&ns, NS_PGTERMS) && local == b"file" {
                    if let Some(f) = file.take() {
                        if let Some(url) = f.url.clone() {
                            if let Some(format) = classify_url(&url) {
                                let entry = RdfFormat {
                                    format,
                                    url: url.clone(),
                                    extent: f.extent,
                                    modified: f.modified,
                                };
                                // /cache/epub/ URLs match the rsync module —
                                // prefer them over any /files/ duplicate.
                                let replace = book
                                    .formats
                                    .iter()
                                    .position(|x| x.format == format)
                                    .filter(|_| url.contains("/cache/epub/"));
                                match replace {
                                    Some(i) => book.formats[i] = entry,
                                    None if !book.formats.iter().any(|x| x.format == format) => {
                                        book.formats.push(entry)
                                    }
                                    None => {}
                                }
                            }
                        }
                    }
                } else if ns_matches(&ns, NS_PGTERMS) && local == b"ebook" {
                    in_ebook = false;
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }

    if book.id == 0 {
        anyhow::bail!("no pgterms:ebook id found in RDF");
    }
    if book.title.is_empty() {
        anyhow::bail!("no dcterms:title found in RDF for ebook {}", book.id);
    }
    Ok(book)
}

fn reader_event_local(event: &Event<'_>, local: &[u8]) -> bool {
    match event {
        Event::Start(e) | Event::Empty(e) => e.name().local_name().as_ref() == local,
        _ => false,
    }
}

fn ns_matches(ns: &ResolveResult<'_>, uri: &[u8]) -> bool {
    matches!(ns, ResolveResult::Bound(u) if u.as_ref() == uri)
}

/// Attribute lookup by local name (works whatever the prefix is).
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
            quick_xml::escape::unescape(&raw).ok().map(|v| v.into_owned())
        })
}

fn classify_url(url: &str) -> Option<Format> {
    for f in Format::ALL {
        if f.matches_url(url) {
            return Some(f);
        }
    }
    None
}
