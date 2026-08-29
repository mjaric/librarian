//! App chrome: the top bar (brand + Home/Categories, active route marked)
//! and the quiet footer. Plain `<a>`s with manual active tracking keep the
//! exact-match semantics of the nav.

use leptos::prelude::*;
use leptos_router::hooks::use_location;

#[component]
pub fn Shell(children: Children) -> impl IntoView {
    let location = use_location();
    let path = Memo::new(move |_| location.pathname.get());
    let home_on = move || path.get() == "/";
    let cats_on = move || path.get() == "/categories";

    view! {
        <div class="page">
            <header class="topbar">
                <div class="topbar-in">
                    <a class="brand" href="/" aria-label="librarian home">
                        <span class="brand-spines" aria-hidden="true">
                            <i></i><i></i><i></i>
                        </span>
                        <span class="brand-name">"librarian"</span>
                    </a>
                    <nav class="nav" aria-label="Main">
                        <a class="nav-link" class:active=home_on href="/">"Home"</a>
                        <a class="nav-link" class:active=cats_on href="/categories">"Categories"</a>
                    </nav>
                    <div class="topbar-tag">"a private mirror"</div>
                </div>
            </header>
            <main class="main">{children()}</main>
            <footer class="foot">
                <span>"librarian — Project Gutenberg, mirrored politely"</span>
            </footer>
        </div>
    }
}
