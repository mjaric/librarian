//! Plain-text opener for `pg{id}.txt`: one section, one block per
//! paragraph. Gutenberg's hard-wrapped lines join into their paragraph,
//! and the PG boilerplate header/footer stay IN the output on purpose —
//! stripping them is a deliberate future decision (indexing overlap),
//! not an oversight.

use crate::Reader;
use crate::doc::{BlockSpan, Section};

/// UTF-8 BOM, if the file carries one.
const BOM: &str = "\u{feff}";

pub fn open_txt(bytes: &[u8]) -> anyhow::Result<Reader> {
    let decoded = String::from_utf8_lossy(bytes);
    let decoded = decoded.strip_prefix(BOM).unwrap_or(&decoded);
    // Paragraph grouping happens on line endings, before normalization
    // erases them: a CRLF blank line never contains a bare "\n\n".
    let lf = decoded.replace("\r\n", "\n").replace('\r', "\n");

    let mut text = String::new();
    let mut blocks: Vec<BlockSpan> = Vec::new();
    let mut para = String::new();
    for line in lf.split('\n') {
        if line.trim().is_empty() {
            flush_para(&mut para, &mut text, &mut blocks);
        } else {
            if !para.is_empty() {
                para.push(' '); // hard-wrapped line joins, normalize collapses
            }
            para.push_str(line);
        }
    }
    flush_para(&mut para, &mut text, &mut blocks);

    let section = Section {
        id: "txt:0".into(),
        title: None,
        html: String::new(), // txt renders from blocks alone
        text: ropey::Rope::from_str(&text),
        blocks,
        source: None, // plain text carries no links
    };
    Ok(Reader::from_parts(
        crate::doc::SourceFormat::Txt,
        vec![section],
        Vec::new(),
        std::collections::BTreeMap::new(),
    ))
}

/// Normalize one accumulated paragraph into the section text, closing a
/// block span over it. Blank/garbage paragraphs contribute nothing.
fn flush_para(para: &mut String, text: &mut String, blocks: &mut Vec<BlockSpan>) {
    if !para.is_empty() {
        let normalized = crate::normalize::normalize(para);
        if !normalized.is_empty() {
            if !text.is_empty() {
                text.push(' ');
            }
            let start = text.chars().count();
            text.push_str(&normalized);
            blocks.push(BlockSpan {
                start,
                end: text.chars().count(),
            });
        }
        para.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anchor::anchor_at;

    #[test]
    fn paragraphs_become_blocks() {
        let r = open_txt(b"First para line one\nline two.\n\nSecond para.\n\n\nThird.").unwrap();
        let s = &r.sections()[0];
        assert_eq!(s.id, "txt:0");
        assert_eq!(s.blocks.len(), 3);
        let text: String = s.text.to_string();
        assert_eq!(text, "First para line one line two. Second para. Third.");
        // Spans are non-overlapping and cover each paragraph.
        assert_eq!(
            &text[s.blocks[0].start..s.blocks[0].end],
            "First para line one line two."
        );
        assert_eq!(&text[s.blocks[1].start..s.blocks[1].end], "Second para.");
    }

    #[test]
    fn crlf_paragraph_breaks_are_seen() {
        let r = open_txt(b"Para one.\r\n\r\nPara two.\r\n").unwrap();
        assert_eq!(r.sections()[0].blocks.len(), 2);
    }

    #[test]
    fn bom_is_stripped() {
        let mut bytes = b"\xef\xbb\xbfTitle line\n".to_vec();
        bytes.extend(b"\nBody follows.");
        let r = open_txt(&bytes).unwrap();
        let s = &r.sections()[0];
        assert!(!s.text.to_string().starts_with('\u{feff}'));
        assert_eq!(s.blocks.len(), 2);
    }

    #[test]
    fn boilerplate_stays_intact() {
        let doc = b"The Project Gutenberg eBook of Dracula\n\n*** START OF ...\n\nStory.\n\n*** END ...\n";
        let r = open_txt(doc).unwrap();
        let text = r.sections()[0].text.to_string();
        // Deliberate: header/footer stripping is a future indexing decision.
        assert!(text.starts_with("The Project Gutenberg eBook"));
        assert!(text.ends_with("*** END ..."));
    }

    #[test]
    fn empty_input_is_one_empty_section_not_an_error() {
        let r = open_txt(b"").unwrap();
        let s = &r.sections()[0];
        assert_eq!(s.blocks.len(), 0);
        assert_eq!(s.text.len_chars(), 0);
        assert_eq!(anchor_at(s, 100).start, 0);
    }
}
