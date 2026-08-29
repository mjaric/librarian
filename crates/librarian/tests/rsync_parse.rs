//! Itemize lines → `(book_id, format)` mapping, events split (transfers /
//! removals), and the rdf ingest set — the gutenberg-side view over
//! bookshelf-core's parsed itemize output.

use bookshelf_core::parse_itemize;
use librarian::gutenberg_org::rdf::{Format, MirrorEntry, parse_mirror_name};

const LINES: &[&str] = &[
    "<f.st......|1342/pg1342-images.epub|24846294",
    ">f+++++++++|51564/pg51564.rdf|18220",
    "*deleting|1342/pg1342-h.zip|0",
];

#[test]
fn maps_fixture_lines_to_books_and_formats() {
    let mut transfers = Vec::new();
    let mut removals = Vec::new();
    let mut rdf_ids = Vec::new();

    for raw in LINES {
        let line = parse_itemize(raw).unwrap();
        let (book_id, entry) = parse_mirror_name(&line.name).unwrap();
        if line.is_deletion() {
            removals.push((book_id, entry));
        } else if line.is_file_transfer() {
            match entry {
                MirrorEntry::Rdf => rdf_ids.push(book_id),
                f @ MirrorEntry::Format(_) => transfers.push((book_id, f, line.bytes)),
            }
        }
    }

    // transfers: exactly the epub.images file for book 1342
    assert_eq!(transfers.len(), 1);
    assert_eq!(transfers[0].0, 1342);
    assert_eq!(transfers[0].1, MirrorEntry::Format(Format::EpubImages));
    assert_eq!(transfers[0].2, 24846294);

    // ingest set: the rdf id
    assert_eq!(rdf_ids, vec![51564]);

    // removals: html.zip for 1342
    assert_eq!(removals.len(), 1);
    assert_eq!(removals[0].0, 1342);
    assert_eq!(removals[0].1, MirrorEntry::Format(Format::HtmlZip));
}
