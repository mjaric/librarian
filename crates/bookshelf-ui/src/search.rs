//! Search results: the same slip, compact, above the card grid.

use bookshelf_api::BookHit;
use leptos::prelude::*;
use leptos_router::hooks::use_query_map;

use crate::api;
use crate::book_card::BookCard;
use crate::load::load;
use crate::search_widget::SearchWidget;

#[component]
pub fn Search() -> impl IntoView {
    let query = use_query_map();
    let q = Memo::new(move |_| api::urldecode(&query.get().get("q").unwrap_or_default()));
    let scope = Memo::new(move |_| api::urldecode(&query.get().get("scope").unwrap_or_default()));

    let hits = RwSignal::new(None::<Result<Vec<BookHit>, String>>);
    load(
        move || (q.get(), scope.get()),
        |(q, scope)| async move { api::search(&q, &scope).await },
        hits,
    );

    view! {
        <section class="searchpage">
            <SearchWidget initial_q=q.get_untracked() initial_scope=scope.get_untracked() compact=true />
            {move || {
                hits.get()
                    .map(|res| match res {
                        Err(e) => view! { <p class="error">{e}</p> }.into_any(),
                        Ok(list) if list.is_empty() => view! {
                            <div class="empty">
                                <p class="empty-title">"Nothing under that query."</p>
                                <p class="empty-note">
                                    "Try fewer words, or switch the scope to All —
                                     titles and authors are searched whole."
                                </p>
                            </div>
                        }
                        .into_any(),
                        Ok(list) => {
                            let n = list.len();
                            view! {
                                <p class="result-line">
                                    {format!("{} {}", n, if n == 1 { "match" } else { "matches" })}
                                </p>
                                <div class="cards">
                                    {list
                                        .into_iter()
                                        .enumerate()
                                        .map(|(i, h)| view! { <BookCard hit=h index=i /> })
                                        .collect::<Vec<_>>()}
                                </div>
                            }
                                .into_any()
                        }
                    })
                    .unwrap_or_else(|| {
                        view! {
                            <div class="loading" role="status" aria-live="polite">
                                <span class="spinner" aria-hidden="true"></span>
                                <span>"Searching the stacks…"</span>
                            </div>
                        }
                            .into_any()
                    })
            }}
        </section>
    }
}
