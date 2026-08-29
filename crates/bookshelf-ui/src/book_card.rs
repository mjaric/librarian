//! The two book widgets: the catalog card (grids, search results) and the
//! book spine (recent strip). A card borrows the ruled layout of a physical
//! catalog card — title above the rule, facts below it.

use bookshelf_api::BookHit;
use leptos::prelude::*;

use crate::util::{authors_line, cloth_style, thousands};

#[component]
pub fn BookCard(hit: BookHit, #[prop(optional)] index: usize) -> impl IntoView {
    let year = hit
        .year
        .map(|y| y.to_string())
        .unwrap_or_else(|| "n.d.".into());
    // Precomputed: the view closures must own plain data, not borrow `hit`.
    let stamps: Vec<(String, String)> = hit
        .categories
        .iter()
        .take(2)
        .map(|c| (c.clone(), cloth_style(c)))
        .collect();
    let has_stamps = !stamps.is_empty();
    view! {
        <a
            class="card"
            href=format!("/books/{}", hit.id)
            style=format!("animation-delay:{}ms", (index % 12) * 35)
        >
            <span class="card-no">{format!("{:05}", hit.id)}</span>
            <h3 class="card-title">{hit.title.clone()}</h3>
            <div class="card-rule"></div>
            <p class="card-author">{authors_line(&hit.authors)}</p>
            <p class="card-facts">
                <span>{year}</span>
                <span class="dot">{"·"}</span>
                <span>{hit.language.to_uppercase()}</span>
                <span class="dot">{"·"}</span>
                <span>{thousands(hit.downloads)}</span>
                <span class="muted">" dl"</span>
            </p>
            <Show when=move || has_stamps>
                <div class="card-shelves">
                    {stamps
                        .clone()
                        .into_iter()
                        .map(|(name, style)| {
                            view! { <span class="stamp" style=style>{name}</span> }
                        })
                        .collect::<Vec<_>>()}
                </div>
            </Show>
        </a>
    }
}

/// A vertical spine for the "fresh on the shelf" shelf: deterministic cloth
/// color, thickness from the txt size (page-count proxy), height varying
/// per book — bands and shading mimic a real binding.
#[component]
pub fn BookSpine(hit: BookHit) -> impl IntoView {
    let (w, h) = crate::util::spine_geometry(hit.id, hit.txt_bytes);
    let style = format!(
        "{}; width:{w}px; height:{h}px",
        cloth_style(&format!("spine-{}", hit.id))
    );
    let tooltip = format!(
        "{} — {}",
        hit.title,
        crate::util::human_bytes(hit.txt_bytes)
    );
    view! {
        <a class="spine book-spine" style=style.clone() href=format!("/books/{}", hit.id) title=tooltip>
            <span class="spine-label">{hit.title.clone()}</span>
            <span class="spine-foot">{format!("{:05}", hit.id)}</span>
        </a>
    }
}
