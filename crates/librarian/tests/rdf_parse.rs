//! Parse the pg1342.rdf fixture (verbatim from www.gutenberg.org) and assert
//! every extracted field exactly.

use librarian::gutenberg_org::rdf::{Format, MirrorEntry, parse_mirror_name, parse_rdf};

fn fixture() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pg1342.rdf");
    std::fs::read_to_string(&path).unwrap()
}

#[test]
fn parses_pg1342_exactly() {
    let book = parse_rdf(&fixture()).unwrap();

    assert_eq!(book.id, 1342);
    assert_eq!(book.r#type, "Text");
    assert_eq!(book.title, "Pride and Prejudice");
    assert_eq!(book.language, "en");
    assert_eq!(book.issued.as_deref(), Some("1998-06-01"));
    assert_eq!(book.issued_date().unwrap().to_string(), "1998-06-01");
    assert_eq!(book.publisher.as_deref(), Some("Project Gutenberg"));
    assert_eq!(book.rights.as_deref(), Some("Public domain in the USA."));
    assert_eq!(book.downloads, Some(183505));
    assert!(
        book.description
            .as_deref()
            .unwrap()
            .contains("automatically generated summary")
    );
    assert!(
        book.reading_ease
            .as_deref()
            .unwrap()
            .contains("Reading ease score: 69.2")
    );

    // authors
    assert_eq!(book.authors.len(), 1);
    let a = &book.authors[0];
    assert_eq!(a.name, "Austen, Jane");
    assert_eq!(a.birth, Some(1775));
    assert_eq!(a.death, Some(1817));
    assert_eq!(
        a.wikipedia.as_deref(),
        Some("https://en.wikipedia.org/wiki/Jane_Austen")
    );
    let authors_json = serde_json::to_value(&book.authors).unwrap();
    assert_eq!(
        authors_json,
        serde_json::json!([{
            "name": "Austen, Jane",
            "birth": 1775,
            "death": 1817,
            "wikipedia": "https://en.wikipedia.org/wiki/Jane_Austen"
        }])
    );

    // subjects: 7 LCSH + 1 LCC
    let lcsh: Vec<_> = book
        .subjects
        .iter()
        .filter(|s| s.scheme == "LCSH")
        .collect();
    let lcc: Vec<_> = book.subjects.iter().filter(|s| s.scheme == "LCC").collect();
    assert_eq!(lcsh.len(), 7);
    assert_eq!(lcc.len(), 1);
    assert_eq!(lcc[0].value, "PR");
    assert!(lcsh.iter().any(|s| s.value == "Love stories"));

    // bookshelves: raw values, prefix intact
    assert!(book.bookshelves.contains(&"Category: Romance".to_string()));
    assert!(book.bookshelves.contains(&"Harvard Classics".to_string()));
    assert_eq!(book.bookshelves.len(), 6);

    // hasFormat: exactly our 4 formats with extents
    assert_eq!(book.formats.len(), 4);
    let epub = book.format_entry(Format::EpubImages).unwrap();
    assert_eq!(
        epub.url,
        "https://www.gutenberg.org/ebooks/1342.epub.images"
    );
    assert_eq!(epub.extent, Some(24846290));
    let txt = book.format_entry(Format::Txt).unwrap();
    assert_eq!(txt.extent, Some(772386));
    assert_eq!(txt.url, "https://www.gutenberg.org/ebooks/1342.txt.utf-8");
    let zip = book.format_entry(Format::HtmlZip).unwrap();
    assert_eq!(zip.extent, Some(25269569));
    assert_eq!(
        zip.url,
        "https://www.gutenberg.org/cache/epub/1342/pg1342-h.zip"
    );
    let cover = book.format_entry(Format::Cover).unwrap();
    assert_eq!(cover.extent, Some(31675));
    assert!(cover.url.contains("/cache/epub/"));
    for f in &book.formats {
        assert!(f.modified.is_some());
    }
}

#[test]
fn epub3_images_is_not_epub_images() {
    assert!(Format::EpubImages.matches_url("https://www.gutenberg.org/ebooks/1342.epub.images"));
    assert!(!Format::EpubImages.matches_url("https://www.gutenberg.org/ebooks/1342.epub3.images"));
}

#[test]
fn mirror_names_map_roundtrip() {
    assert_eq!(Format::Txt.mirror_name(1342), "pg1342.txt");
    assert_eq!(Format::EpubImages.mirror_name(1342), "pg1342-images.epub");
    assert_eq!(Format::HtmlZip.mirror_name(1342), "pg1342-h.zip");
    assert_eq!(Format::Cover.mirror_name(1342), "pg1342.cover.medium.jpg");

    assert_eq!(
        parse_mirror_name("1342/pg1342-images.epub"),
        Some((1342, MirrorEntry::Format(Format::EpubImages)))
    );
    assert_eq!(
        parse_mirror_name("1342/pg1342.cover.medium.jpg"),
        Some((1342, MirrorEntry::Format(Format::Cover)))
    );
    assert_eq!(
        parse_mirror_name("pg51564.rdf"),
        Some((51564, MirrorEntry::Rdf))
    );
    assert_eq!(parse_mirror_name("1342/LICENSE.txt"), None);
    assert_eq!(parse_mirror_name("1342/1342-0.txt"), None);
    assert_eq!(parse_mirror_name("1342/pg1342-images-3.epub"), None);
}
