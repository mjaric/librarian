//! Category taxonomy. PG's categories page has 9 top groups; the leaf set
//! in RDF (`pgterms:bookshelf` values `Category: X`) is the source of truth.
//! This seed — a snapshot of https://www.gutenberg.org/ebooks/categories
//! captured 2026-08-28 — only supplies top groups + bookshelf ids; unknown
//! RDF leaves get parent NULL and are reported under `Unassigned`. Seed
//! updates (PG tree reorg — rare) are manual code changes.

/// (top group, leaf name, bookshelf id from /ebooks/bookshelf/{id})
pub const SEED: &[(&str, &str, i32)] = &[
    // Literature
    ("Literature", "Adventure", 644),
    ("Literature", "American Literature", 654),
    ("Literature", "British Literature", 653),
    ("Literature", "French Literature", 652),
    ("Literature", "German Literature", 651),
    ("Literature", "Russian Literature", 650),
    ("Literature", "Classics of Literature", 649),
    ("Literature", "Biographies", 643),
    ("Literature", "Novels", 645),
    ("Literature", "Short Stories", 634),
    ("Literature", "Poetry", 637),
    ("Literature", "Plays/Films/Dramas", 642),
    ("Literature", "Romance", 639),
    ("Literature", "Science-Fiction & Fantasy", 638),
    ("Literature", "Crime, Thrillers & Mystery", 640),
    ("Literature", "Mythology, Legends & Folklore", 646),
    ("Literature", "Humour", 641),
    ("Literature", "Children & Young Adult Reading", 636),
    ("Literature", "Literature - Other", 633),
    // Science & Technology
    ("Science & Technology", "Engineering & Technology", 671),
    ("Science & Technology", "Mathematics", 672),
    ("Science & Technology", "Science - Physics", 667),
    ("Science & Technology", "Science - Chemistry/Biochemistry", 668),
    ("Science & Technology", "Science - Biology", 669),
    ("Science & Technology", "Science - Earth/Agricultural/Farming", 670),
    ("Science & Technology", "Research Methods/Statistics/Information Sys", 673),
    ("Science & Technology", "Environmental Issues", 685),
    // History
    ("History", "History - American", 656),
    ("History", "History - British", 657),
    ("History", "History - European", 658),
    ("History", "History - Ancient", 659),
    ("History", "History - Medieval/Middle Ages", 660),
    ("History", "History - Early Modern (c. 1450-1750)", 661),
    ("History", "History - Modern (1750+)", 662),
    ("History", "History - Religious", 663),
    ("History", "History - Royalty", 664),
    ("History", "History - Warfare", 665),
    ("History", "History - Schools & Universities", 666),
    ("History", "History - Other", 655),
    ("History", "Archaeology & Anthropology", 686),
    // Social Sciences & Society
    ("Social Sciences & Society", "Business/Management", 695),
    ("Social Sciences & Society", "Economics", 696),
    ("Social Sciences & Society", "Law & Criminology", 689),
    ("Social Sciences & Society", "Gender & Sexuality Studies", 690),
    ("Social Sciences & Society", "Psychiatry/Psychology", 688),
    ("Social Sciences & Society", "Sociology", 693),
    ("Social Sciences & Society", "Politics", 694),
    ("Social Sciences & Society", "Parenthood & Family Relations", 701),
    ("Social Sciences & Society", "Old Age & the Elderly", 700),
    // Arts & Culture
    ("Arts & Culture", "Art", 675),
    ("Arts & Culture", "Architecture", 674),
    ("Arts & Culture", "Music", 677),
    ("Arts & Culture", "Fashion", 676),
    ("Arts & Culture", "Journalism/Media/Writing", 698),
    ("Arts & Culture", "Language & Communication", 687),
    ("Arts & Culture", "Essays, Letters & Speeches", 647),
    // Religion & Philosophy
    ("Religion & Philosophy", "Religion/Spirituality", 692),
    ("Religion & Philosophy", "Philosophy & Ethics", 691),
    // Lifestyle & Hobbies
    ("Lifestyle & Hobbies", "Cooking & Drinking", 678),
    ("Lifestyle & Hobbies", "Sports/Hobbies", 680),
    ("Lifestyle & Hobbies", "How To ...", 679),
    ("Lifestyle & Hobbies", "Travel Writing", 648),
    ("Lifestyle & Hobbies", "Nature/Gardening/Animals", 683),
    ("Lifestyle & Hobbies", "Sexuality & Erotica", 703),
    // Health & Medicine
    ("Health & Medicine", "Health & Medicine", 681),
    ("Health & Medicine", "Drugs/Alcohol/Pharmacology", 682),
    ("Health & Medicine", "Nutrition", 684),
    // Education & Reference
    ("Education & Reference", "Encyclopedias/Dictionaries/Reference", 697),
    ("Education & Reference", "Teaching & Education", 704),
    ("Education & Reference", "Reports & Conference Proceedings", 702),
    ("Education & Reference", "Journals", 699),
];

/// `Category: Romance` → `Romance`. Non-category shelves → None.
pub fn strip_category(shelf: &str) -> Option<&str> {
    shelf.strip_prefix("Category: ")
}

/// Top group for a seed-known leaf; None for unknown leaves (they get a
/// NULL parent and are reported under the synthetic group `Unassigned`).
pub fn seed_parent(leaf: &str) -> Option<&'static str> {
    SEED.iter().find(|(_, l, _)| *l == leaf).map(|(p, _, _)| *p)
}

/// All category leaves referenced by one book's raw shelf list.
pub fn category_leaves(shelves: &[String]) -> Vec<&str> {
    shelves.iter().filter_map(|s| strip_category(s)).collect()
}

