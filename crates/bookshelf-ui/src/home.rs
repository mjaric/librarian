//! Home: the reading desk. One question, one dominating search slip, the
//! catalog's vital line, quick stacks to browse, and the fresh-arrivals
//! shelf rendered like real books.

use bookshelf_api::{BookHit, CategoryGroup, Stats};
use leptos::prelude::*;

use crate::api;
use crate::book_card::BookSpine;
use crate::load::load;
use crate::search_widget::SearchWidget;
use crate::util::{cloth_style, thousands};

#[component]
pub fn Home() -> impl IntoView {
    let stats = RwSignal::new(None::<Result<Stats, String>>);
    let recent = RwSignal::new(None::<Result<Vec<BookHit>, String>>);
    let groups = RwSignal::new(None::<Result<Vec<CategoryGroup>, String>>);
    load(|| (), |_| async { api::stats().await }, stats);
    load(|| (), |_| async { api::recent(12).await }, recent);
    load(|| (), |_| async { api::categories().await }, groups);

    view! {
        <section class="hero">
            <p class="eyebrow">"Private catalogue · Project Gutenberg mirror"</p>
            <h1 class="hero-title">"What will you read next?"</h1>
            <SearchWidget initial_q=String::new() initial_scope="all".to_string() />
            {move || {
                stats
                    .get()
                    .map(|res| match res {
                        Err(e) => view! { <p class="error">{e}</p> }.into_any(),
                        Ok(s) => {
                            let last = s.last_sync.unwrap_or_else(|| "never".into());
                            view! {
                                <p class="vital">
                                    <strong>{thousands(Some(s.books))}</strong>
                                    " books on the shelves · "
                                    <strong>{thousands(Some(s.categories))}</strong>
                                    " categories · last synced "
                                    {last}
                                </p>
                            }
                                .into_any()
                        }
                    })
                    .unwrap_or_else(|| view! { <p class="vital">"…"</p> }.into_any())
            }}
            // Quick stacks: the fullest groups, straight from the hero.
            {move || {
                groups
                    .get()
                    .map(|res| match res {
                        Err(_) => ().into_any(),
                        Ok(groups) => {
                            let chips: Vec<_> = groups
                                .iter()
                                .take(5)
                                .map(|g| {
                                    view! {
                                        <a
                                            class="hero-chip"
                                            style=cloth_style(&g.group)
                                            href=format!(
                                                "/categories?group={}",
                                                api::urlencode(&g.group)
                                            )
                                        >
                                            {g.group.clone()}
                                        </a>
                                    }
                                })
                                .collect();
                            view! { <nav class="hero-chips" aria-label="Browse by group">{chips}</nav> }
                                .into_any()
                        }
                    })
                    .unwrap_or_else(|| ().into_any())
            }}
        </section>

        <section class="fresh">
            <div class="fresh-head">
                <h2 class="section-title">"Fresh on the shelf"</h2>
                <span class="section-note">
                    "the latest arrivals — spine width follows length, no two heights alike"
                </span>
            </div>
            {move || {
                recent
                    .get()
                    .map(|res| match res {
                        Err(e) => view! { <p class="error">{e}</p> }.into_any(),
                        Ok(hits) => {
                            let spines: Vec<_> = hits
                                .into_iter()
                                .take(12)
                                .map(|h| view! { <BookSpine hit=h /> })
                                .collect();
                            view! {
                                <div class="shelfcase">
                                    <div class="spines">{spines}</div>
                                    <div class="board" aria-hidden="true"></div>
                                </div>
                            }
                                .into_any()
                        }
                    })
                    .unwrap_or_else(|| view! { <div class="shelfcase"></div> }.into_any())
            }}
        </section>
    }
}
