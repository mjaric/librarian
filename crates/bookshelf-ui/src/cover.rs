//! Generated covers: when the mirror has no cover image, the book gets a
//! typographic half-binding in its deterministic cloth color — never a gray
//! placeholder box.

use leptos::prelude::*;

use crate::util::{authors_line, cloth_style};

#[component]
pub fn GeneratedCover(
    id: i64,
    title: String,
    authors: Vec<String>,
    #[prop(optional)] wide: bool,
) -> impl IntoView {
    let author = authors_line(&authors);
    view! {
        <div class="cover gen" class:wide=wide style=cloth_style(&format!("cover-{id}"))>
            <div class="gen-band"></div>
            <p class="gen-title">{title.clone()}</p>
            <div class="gen-rule"></div>
            <p class="gen-author">{author}</p>
            <p class="gen-no">{format!("№ {id}")}</p>
        </div>
    }
}
