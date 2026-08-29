//! Taxonomy seed sanity, `Category: X` stripping, parent resolution for
//! seed-known vs unknown leaves (unknown → NULL parent → reported as
//! `Unassigned`, and collected into run-level taxonomy.updated).

use std::collections::HashSet;

use librarian::gutenberg_org::taxonomy::{SEED, category_leaves, seed_parent, strip_category};

#[test]
fn seed_has_unique_leaves_and_nine_groups() {
    let mut leaves = HashSet::new();
    let mut groups = HashSet::new();
    for (parent, leaf, id) in SEED {
        assert!(leaves.insert(*leaf), "duplicate leaf {leaf}");
        groups.insert(*parent); // repeats expected: many leaves per group
        assert!(*id > 0, "bookshelf id for {leaf}");
    }
    assert_eq!(groups.len(), 9);
}

#[test]
fn strips_category_prefix_only() {
    assert_eq!(strip_category("Category: Romance"), Some("Romance"));
    assert_eq!(
        strip_category("Category: Science - Physics"),
        Some("Science - Physics")
    );
    assert_eq!(strip_category("Harvard Classics"), None);
    assert_eq!(strip_category("Best Books Ever Listings"), None);
}

#[test]
fn known_leaf_resolves_parent_unknown_is_null() {
    // seed-known leaf → parent resolved
    assert_eq!(seed_parent("Romance"), Some("Literature"));
    assert_eq!(seed_parent("Mathematics"), Some("Science & Technology"));
    assert_eq!(seed_parent("Health & Medicine"), Some("Health & Medicine"));
    // unknown leaf → NULL parent (reported under the synthetic group
    // `Unassigned`); first sight collects into taxonomy.updated.new_leaves
    assert_eq!(seed_parent("Totally New Shelf"), None);

    // the run-level collector: unknown leaves land in new_leaves, known do not
    let shelves = vec![
        "Category: Romance".to_string(),
        "Category: Totally New Shelf".to_string(),
        "Harvard Classics".to_string(),
    ];
    let mut new_leaves: Vec<String> = Vec::new();
    for leaf in category_leaves(&shelves) {
        if seed_parent(leaf).is_none() {
            new_leaves.push(leaf.to_string());
        }
    }
    assert_eq!(new_leaves, vec!["Totally New Shelf".to_string()]);
}

#[test]
fn plan_examples_present_in_seed() {
    // exact examples from the planning snapshot
    assert!(SEED.contains(&("Literature", "Romance", 639)));
    assert!(SEED.contains(&("Science & Technology", "Mathematics", 672)));
}
