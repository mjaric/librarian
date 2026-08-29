//! The search widget — the claude.ai-style "request slip": one dominating
//! input with a tool row at its foot (scope segmented control, surprise-me,
//! submit). Shared by the home hero and the search page.

use leptos::prelude::*;
use leptos_router::hooks::use_navigate;

use crate::api;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    All,
    Title,
    Author,
}

impl Scope {
    fn param(self) -> &'static str {
        match self {
            Scope::All => "all",
            Scope::Title => "title",
            Scope::Author => "author",
        }
    }
    fn from_param(s: &str) -> Scope {
        match s {
            "title" => Scope::Title,
            "author" => Scope::Author,
            _ => Scope::All,
        }
    }
    fn label(self) -> &'static str {
        match self {
            Scope::All => "All",
            Scope::Title => "Title",
            Scope::Author => "Author",
        }
    }
}

#[component]
pub fn SearchWidget(
    initial_q: String,
    initial_scope: String,
    #[prop(optional)] compact: bool,
) -> impl IntoView {
    let q = RwSignal::new(initial_q);
    let scope = RwSignal::new(Scope::from_param(&initial_scope));
    let rolling = RwSignal::new(false);
    let navigate = use_navigate();

    let nav_submit = navigate.clone();
    let submit = move |_| {
        let q = q.get().trim().to_string();
        if q.is_empty() {
            return;
        }
        let url = format!(
            "/search?q={}&scope={}",
            api::urlencode(&q),
            scope.get().param()
        );
        nav_submit(&url, Default::default());
    };

    let nav_surprise = navigate.clone();
    let surprise = move |_| {
        rolling.set(true);
        let nav = nav_surprise.clone();
        leptos::task::spawn_local(async move {
            match api::random().await {
                Ok(hit) => nav(&format!("/books/{}", hit.id), Default::default()),
                Err(e) => {
                    rolling.set(false);
                    leptos::logging::error!("random book: {e}");
                }
            }
        });
    };

    let scopes = [Scope::All, Scope::Title, Scope::Author];

    view! {
        <form class="slip" class:compact=compact on:submit=submit>
            <input
                class="slip-input"
                prop:value=q
                on:input=move |ev| q.set(event_target_value(&ev))
                type="search"
                placeholder="Search the stacks — title or author…"
                autocomplete="off"
                spellcheck="false"
                aria-label="Search the catalog"
            />
            <div class="slip-tools">
                <div class="seg" role="group" aria-label="Search in">
                    {scopes
                        .into_iter()
                        .map(|s| {
                            let is_current = move || scope.get() == s;
                            view! {
                                <button
                                    class="seg-btn"
                                    class:on=is_current
                                    type="button"
                                    aria-pressed=move || is_current().then_some("true")
                                    on:click=move |_| scope.set(s)
                                >
                                    {s.label()}
                                </button>
                            }
                        })
                        .collect::<Vec<_>>()}
                </div>
                <span class="slip-hint">"in"</span>
                <button class="slip-tool" type="button" on:click=surprise disabled=rolling>
                    "✦"
                    <span class="hidden-narrow">"Surprise me"</span>
                </button>
                <span class="slip-grow"></span>
                <button class="slip-find" type="submit">"Find"</button>
            </div>
        </form>
    }
}
