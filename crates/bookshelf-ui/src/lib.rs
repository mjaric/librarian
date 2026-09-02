//! bookshelf-ui — the Leptos catalog front end. Shell-agnostic by design:
//! the web shell (`librarian-web`) serves it as a wasm CSR bundle today; a
//! desktop shell can embed this same crate against the same JSON API. All
//! data comes over HTTP from a bookshelf server — never the DB directly.

pub mod api;
mod book;
mod book_card;
mod categories;
mod cover;
mod home;
mod load;
mod reader;
mod search;
mod search_widget;
mod shell;
pub mod util;

use leptos::prelude::*;
use leptos_router::components::{Route, Router, Routes};
use leptos_router::path;

#[component]
pub fn App() -> impl IntoView {
    view! {
        <Router>
            <shell::Shell>
                <Routes fallback=|| {
                    view! {
                        <div class="empty">
                            <p class="empty-title">"No such page."</p>
                            <p class="empty-note">
                                <a href="/">"Back to the reading desk."</a>
                            </p>
                        </div>
                    }
                }>
                    <Route path=path!("/") view=home::Home />
                    <Route path=path!("/search") view=search::Search />
                    <Route path=path!("/categories") view=categories::Categories />
                    <Route path=path!("/books/:id") view=book::BookView />
                    <Route path=path!("/books/:id/read") view=reader::ReadPick />
                    <Route path=path!("/books/:id/read/:format") view=reader::ReadView />
                </Routes>
            </shell::Shell>
        </Router>
    }
}

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::wasm_bindgen;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn main() {
    // Clear the boot placeholder before taking over the body.
    if let Some(el) = document().get_element_by_id("app") {
        el.set_inner_html("");
    }
    use leptos::mount::mount_to_body;
    mount_to_body(App);
}
