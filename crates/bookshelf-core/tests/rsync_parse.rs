//! Fixture itemize lines (`%i|%n|%b`) → parsed structure + exit-class table.

use bookshelf_core::{classify_exit, parse_itemize, ExitClass};

const LINES: &[&str] = &[
    "<f.st......|1342/pg1342-images.epub|24846294",
    ">f+++++++++|51564/pg51564.rdf|18220",
    "*deleting|1342/pg1342-h.zip|0",
];

#[test]
fn parses_fixture_itemize_lines() {
    let epub = parse_itemize(LINES[0]).unwrap();
    assert_eq!(epub.itemize, "<f.st......");
    assert_eq!(epub.name, "1342/pg1342-images.epub");
    assert_eq!(epub.bytes, 24846294);
    assert!(epub.is_file_transfer());
    assert!(!epub.is_deletion());

    let rdf = parse_itemize(LINES[1]).unwrap();
    assert_eq!(rdf.name, "51564/pg51564.rdf");
    assert_eq!(rdf.bytes, 18220);
    assert!(rdf.is_file_transfer());

    let del = parse_itemize(LINES[2]).unwrap();
    assert!(del.is_deletion());
    assert!(!del.is_file_transfer());
    assert_eq!(del.bytes, 0);
}

#[test]
fn malformed_lines_rejected() {
    assert!(parse_itemize("rsync: connection refused").is_none());
    assert!(parse_itemize("|nobytes|12").is_none());
    assert!(parse_itemize("only|two|parts|ok").is_some()); // extra pipes stay in %b
}

#[test]
fn exit_code_table() {
    assert_eq!(classify_exit(0), ExitClass::Ok);
    assert_eq!(classify_exit(23), ExitClass::Partial);
    assert_eq!(classify_exit(24), ExitClass::Partial);
    for c in [5, 6, 10, 11] {
        assert_eq!(classify_exit(c), ExitClass::Retryable);
    }
    for c in [1, 2, 3, 4, 12, 20, 25, 30] {
        assert_eq!(classify_exit(c), ExitClass::Fatal);
    }
}
