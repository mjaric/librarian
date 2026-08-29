//! The stack room: every top group is a shelf, every leaf a spine whose
//! width follows its book count. The scoped filter thins the wall live;
//! picking a spine opens its card grid above the wall, server-filtered
//! and paged.

use bookshelf_api::{CategoryBooksPage, CategoryGroup, CategoryLeaf};
use leptos::prelude::*;
use leptos_router::hooks::{use_navigate, use_query_map};

use crate::api;
use crate::book_card::BookCard;
use crate::load::{debounce, load};
use crate::util::cloth_style;

/// Cards per "load more" step — one screenful, never a wall.
const STEP: i64 = 24;

#[component]
pub fn Categories() -> impl IntoView {
    let query = use_query_map();
    let navigate = use_navigate();

    // Wall state.
    let tree = RwSignal::new(None::<Result<Vec<CategoryGroup>, String>>);
    load(|| (), |_| async { api::categories().await }, tree);
    let filter = RwSignal::new(String::new()); // client-side leaf filter

    // Selected shelf (URL-driven so it is shareable).
    let shelf = Memo::new(move |_| api::urldecode(&query.get().get("shelf").unwrap_or_default()));
    // Optional group focus from the home quick links (?group=Literature).
    let group = Memo::new(move |_| api::urldecode(&query.get().get("group").unwrap_or_default()));

    // Server-side book filter within the selected shelf, debounced.
    let shelf_q_raw = RwSignal::new(String::new());
    let shelf_q = RwSignal::new(String::new());
    debounce(shelf_q_raw, shelf_q, 250);

    // Window size grows on "load more"; resets when the query changes.
    let limit = RwSignal::new(STEP);
    Effect::new(move |_| {
        let _ = (shelf.get(), shelf_q.get()); // re-run on either change
        limit.set(STEP);
    });

    let page: RwSignal<Option<Result<Option<CategoryBooksPage>, String>>> = RwSignal::new(None);
    load(
        move || (shelf.get(), shelf_q.get(), limit.get()),
        |(shelf, q, limit)| async move {
            if shelf.is_empty() {
                Ok(None)
            } else {
                api::category_books(&shelf, &q, limit).await.map(Some)
            }
        },
        page,
    );

    // StoredValue: a Copy handle, so view/handler closures stay `Fn`.
    let pick = StoredValue::new({
        let navigate = navigate.clone();
        move |leaf: String| {
            let url = if leaf.is_empty() {
                "/categories".to_string()
            } else {
                format!("/categories?shelf={}", api::urlencode(&leaf))
            };
            navigate(&url, Default::default());
        }
    });

    view! {
        <section class="stacks">
            <div class="stacks-head">
                <div>
                    <p class="eyebrow">"The stack room"</p>
                    <h1 class="section-title">"Categories"</h1>
                </div>
                <div class="stacks-tools">
                    <label class="scoped">
                        <span class="scoped-icon" aria-hidden="true">"⌕"</span>
                        <input
                            class="scoped-input"
                            prop:value=filter
                            on:input=move |ev| filter.set(event_target_value(&ev))
                            type="search"
                            placeholder="Filter shelves…"
                            aria-label="Filter categories"
                        />
                    </label>
                    <Show when=move || !shelf.get().is_empty()>
                        <button class="btn-quiet" type="button" on:click=move |_| pick.with_value(|p| p(String::new()))>
                            "× clear shelf"
                        </button>
                    </Show>
                    <Show when=move || !group.get().is_empty()>
                        <button
                            class="btn-quiet"
                            type="button"
                            on:click=move |_| pick.with_value(|p| p(String::new()))
                        >
                            {format!("× {} (all groups)", group.get())}
                        </button>
                    </Show>
                </div>
            </div>

            // Selected shelf panel — above the wall so it lands in view.
            <Show when=move || !shelf.get().is_empty()>
                <div class="shelfpanel">
                    {move || {
                        page.get()
                            .map(|res| match res {
                                Err(e) => view! { <p class="error">{e}</p> }.into_any(),
                                Ok(None) => view! {
                                    <div class="loading" role="status">
                                        <span class="spinner" aria-hidden="true"></span>
                                        <span>"Pulling the shelf…"</span>
                                    </div>
                                }
                                    .into_any(),
                                Ok(Some(p)) => {
                                    let bookshelf_api::CategoryBooksPage {
                                        category,
                                        total,
                                        offset: _,
                                        items,
                                    } = p;
                                    let shown = items.len() as i64;
                                    let panel_style = cloth_style(&category);
                                    let has_more = shown < total;
                                    let any_items = shown > 0;
                                    view! {
                                        <div class="shelfpanel-in" style=panel_style>
                                            <header class="shelfpanel-head">
                                                <h2 class="shelfpanel-title">{category.clone()}</h2>
                                                <span class="shelfpanel-count">
                                                    {format!(
                                                        "{} {}",
                                                        total,
                                                        if total == 1 { "book" } else { "books" },
                                                    )}
                                                </span>
                                                <input
                                                    class="shelfpanel-q"
                                                    prop:value=shelf_q_raw
                                                    on:input=move |ev| shelf_q_raw.set(event_target_value(&ev))
                                                    type="search"
                                                    placeholder="Filter this shelf…"
                                                    aria-label="Filter books on this shelf"
                                                />
                                            </header>
                                            <Show
                                                when=move || any_items
                                                fallback=move || view! {
                                                    <p class="empty-note">
                                                        "Nothing on this shelf matches — loosen the filter."
                                                    </p>
                                                }
                                            >
                                                <div class="cards">
                                                    {items
                                                        .clone()
                                                        .into_iter()
                                                        .enumerate()
                        .map(|(i, h)| view! { <BookCard hit=h index=i /> })
                                                        .collect::<Vec<_>>()}
                                                </div>
                                            </Show>
                                            <Show when=move || has_more>
                                                <div class="shelfpanel-more">
                                                    <button
                                                        class="btn-quiet"
                                                        type="button"
                                                        on:click=move |_| {
                                                            limit.set(limit.get_untracked() + STEP)
                                                        }
                                                    >
                                                        {format!("Load more — showing {} of {}", shown, total)}
                                                    </button>
                                                </div>
                                            </Show>
                                        </div>
                                    }
                                        .into_any()
                                }
                            })
                            .unwrap_or_else(|| view! { <p class="muted">"Pulling the shelf…"</p> }.into_any())
                    }}
                </div>
            </Show>

            // The wall.
            {move || {
                tree.get()
                    .map(|res| match res {
                        Err(e) => view! { <p class="error">{e}</p> }.into_any(),
                        Ok(groups) => {
                            let rows: Vec<_> = visible_rows(groups, &filter.get(), &group.get())
                                .into_iter()
                                .map(|(group, leaves)| {
                                    let group_total: i64 = leaves.iter().map(|l| l.books).sum();
                                    view! {
                                        <section class="shelfrow" style=cloth_style(&group.group)>
                                            <header class="shelfrow-head">
                                                <h2 class="shelfrow-title">{group.group.clone()}</h2>
                                                <span class="shelfrow-count">{group_total}</span>
                                            </header>
                                            <div class="shelf">
                                                {leaves
                                                    .into_iter()
                                                    .map(|leaf| {
                                                        let CategoryLeaf { name, books } = leaf;
                                                        // Copy Memo → cheap `selected` reused by
                                                        // class toggle and click handler alike.
                                                        let name_for_memo = name.clone();
                                                        let selected = Memo::new(
                                                            move |_| shelf.get() == name_for_memo,
                                                        );
                                                        let grow = (books + 1).max(2);
                                                        let tooltip = format!(
                                                            "{} — {} {}",
                                                            name,
                                                            books,
                                                            if books == 1 { "book" } else { "books" },
                                                        );
                                                        let style = format!(
                                                            "{}; flex-grow:{grow}",
                                                            cloth_style(&name),
                                                        );
                                                        view! {
                                                            <button
                                                                class="spine leaf-spine"
                                                                class:selected=move || selected.get()
                                                                style=style
                                                                title=tooltip
                                                                on:click=move |_| {
                                                                    let next = if selected.get_untracked() {
                                                                        String::new()
                                                                    } else {
                                                                        name.clone()
                                                                    };
                                                                    pick.with_value(|p| p(next));
                                                                }
                                                            >
                                                                <span class="spine-label">{name.clone()}</span>
                                                                <span class="spine-count">{books}</span>
                                                            </button>
                                                        }
                                                    })
                                                    .collect::<Vec<_>>()}
                                            </div>
                                        </section>
                                    }
                                })
                                .collect();
                            view! { <div class="wall">{rows}</div> }.into_any()
                        }
                    })
                    .unwrap_or_else(|| {
                        view! {
                            <div class="loading" role="status">
                                <span class="spinner" aria-hidden="true"></span>
                                <span>"Reading the shelves…"</span>
                            </div>
                        }
                            .into_any()
                    })
            }}
        </section>
    }
}

/// (group, matching leaves) for every group that survives the filter and,
/// when set, the group focus.
fn visible_rows(
    groups: Vec<CategoryGroup>,
    filter: &str,
    group: &str,
) -> Vec<(CategoryGroup, Vec<CategoryLeaf>)> {
    let f = filter.to_lowercase();
    groups
        .into_iter()
        .filter(|g| group.is_empty() || g.group.eq_ignore_ascii_case(group))
        .map(|g| {
            let leaves = if f.is_empty() {
                g.leaves.clone()
            } else {
                g.leaves
                    .iter()
                    .filter(|l| {
                        l.name.to_lowercase().contains(&f) || g.group.to_lowercase().contains(&f)
                    })
                    .cloned()
                    .collect()
            };
            (g, leaves)
        })
        .filter(|(_, leaves)| !leaves.is_empty())
        .collect()
}
