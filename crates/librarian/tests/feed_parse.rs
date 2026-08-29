//! Parse the head of today's feed fixture → ids in order + pubDate parsed.

use librarian::gutenberg_org::feed::parse_feed;

fn fixture() -> String {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/today-head.rss");
    std::fs::read_to_string(&path).unwrap()
}

#[test]
fn parses_feed_head() {
    let head = parse_feed(&fixture()).unwrap();
    assert_eq!(
        head.pub_date, "Thu, 27 Aug 2026 17:51:19 -0400",
        "channel pubDate verbatim"
    );
    assert_eq!(head.ids, vec![79455, 79460, 79457, 65688, 37]);

    // pubDate is parseable as RFC 2822 (validated inside parse_feed; prove
    // the format here too)
    let dt = time::OffsetDateTime::parse(
        &head.pub_date,
        &time::format_description::well_known::Rfc2822,
    )
    .unwrap();
    assert_eq!(dt.year(), 2026);
    assert_eq!(
        time::Month::try_from(u8::try_from(dt.month()).unwrap()).unwrap(),
        time::Month::August
    );
}
