//! The book page: an exhibit card. Cover on the left (real when mirrored,
//! typographic cloth otherwise), ruled facts and the publisher's note on the
//! right, formats as a take-out panel at the foot.

use bookshelf_api::{AuthorBio, BookDetail, FileOffer};
use leptos::prelude::*;
use leptos_router::hooks::use_params_map;

use crate::api;
use crate::cover::GeneratedCover;
use crate::load::load;
use crate::util::{cloth_style, ease_label, human_bytes, thousands};

#[component]
pub fn BookView() -> impl IntoView {
    let params = use_params_map();
    let id = Memo::new(move |_| params.get().get("id").and_then(|s| s.parse::<i64>().ok()));

    let detail = RwSignal::new(None::<Result<Option<BookDetail>, String>>);
    load(
        move || id.get(),
        |id| async move {
            match id {
                Some(id) => api::book(id).await,
                None => Ok(None),
            }
        },
        detail,
    );

    view! {
        {move || {
            detail
                .get()
                .map(|res| match res {
                    Err(e) => view! { <p class="error">{e}</p> }.into_any(),
                    Ok(None) => view! {
                        <div class="empty">
                            <p class="empty-title">"No such book."</p>
                            <p class="empty-note">
                                "The catalogue number in the address is not on file."
                            </p>
                        </div>
                    }
                    .into_any(),
                    Ok(Some(d)) => view! { <BookPage d=d /> }.into_any(),
                })
                .unwrap_or_else(|| view! { <p class="muted">"Fetching the card…"</p> }.into_any())
        }}
    }
}

#[component]
fn BookPage(d: BookDetail) -> impl IntoView {
    // View closures must own plain data; borrow nothing from `d`.
    let img_failed = RwSignal::new(false);
    let cover_src = format!("/api/covers/{}", d.id);
    let cover_alt = format!("Cover of {}", d.title);
    let eyebrow = format!(
        "Ebook № {} · {} · Project Gutenberg",
        d.id,
        d.language.to_uppercase()
    );
    let title = d.title.clone();
    let has_real_cover = d.has_cover;
    let cover_id = d.id;
    let cover_title = d.title.clone();
    let cover_authors: Vec<String> = d.authors.iter().map(|a| a.name.clone()).collect();
    let authors: Vec<AuthorBio> = d.authors.clone();
    let has_authors = !authors.is_empty();

    let f_issued = d.year.map(|y| y.to_string());
    let f_language = Some(d.language.to_uppercase());
    let f_downloads = d.downloads.map(|n| thousands(Some(n)));
    let f_ease = d
        .reading_ease
        .map(|s| format!("{s:.0} — {}", ease_label(s)));
    let f_publisher = d.publisher.clone();
    let f_rights = d.rights.clone();

    let note_paragraphs: Vec<String> = d
        .description
        .as_deref()
        .unwrap_or_default()
        .split('\n')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();
    let has_note = !note_paragraphs.is_empty();

    let shelf_chips: Vec<(String, String, String)> = d
        .categories
        .iter()
        .map(|c| {
            (
                c.clone(),
                cloth_style(c),
                format!("/categories?shelf={}", api::urlencode(c)),
            )
        })
        .collect();
    let has_shelves = !shelf_chips.is_empty();

    let subjects: Vec<String> = d.subjects.clone();
    let has_subjects = !subjects.is_empty();

    let files: Vec<FileOffer> = d.files.clone();
    let book_id = d.id;
    let has_files = !files.is_empty();

    view! {
        <article class="bookpage">
            <aside class="book-cover">
                <Show
                    when=move || has_real_cover && !img_failed.get()
                    fallback=move || {
                        view! {
                            <GeneratedCover id=cover_id title=cover_title.clone() authors=cover_authors.clone() />
                        }
                    }
                >
                    <img
                        class="cover img"
                        src=cover_src.clone()
                        alt=cover_alt.clone()
                        on:error=move |_| img_failed.set(true)
                    />
                </Show>
            </aside>

            <div class="book-main">
                <p class="eyebrow">{eyebrow.clone()}</p>
                <h1 class="book-title">{title.clone()}</h1>
                <p class="book-authors">
                    {authors
                        .clone()
                        .into_iter()
                        .map(|a| view! { <AuthorLine a=a /> })
                        .collect::<Vec<_>>()}
                    <Show when=move || !has_authors>
                        <span class="muted">"Unknown author"</span>
                    </Show>
                </p>

                <dl class="book-facts">
                    <Fact label="Issued" value=f_issued.clone() />
                    <Fact label="Language" value=f_language.clone() />
                    <Fact label="Downloads" value=f_downloads.clone() />
                    <Fact label="Reading ease" value=f_ease.clone() />
                    <Fact label="Publisher" value=f_publisher.clone() />
                    <Fact label="License" value=f_rights.clone() />
                </dl>

                <Show when=move || has_note>
                    <section class="book-note">
                        <h2 class="note-title">"Publisher's note"</h2>
                        {note_paragraphs
                            .clone()
                            .into_iter()
                            .map(|p| view! { <p class="note-p">{p}</p> })
                            .collect::<Vec<_>>()}
                    </section>
                </Show>

                <Show when=move || has_shelves>
                    <section class="book-tags">
                        <h2 class="note-title">"Shelved under"</h2>
                        <div class="chips">
                            {shelf_chips
                                .clone()
                                .into_iter()
                                .map(|(name, style, href)| {
                                    view! { <a class="chip" style=style href=href>{name}</a> }
                                })
                                .collect::<Vec<_>>()}
                        </div>
                    </section>
                </Show>

                <Show when=move || has_subjects>
                    <section class="book-tags">
                        <h2 class="note-title">"Subjects"</h2>
                        <div class="chips">
                            {subjects
                                .clone()
                                .into_iter()
                                .map(|s| view! { <span class="chip subject">{s}</span> })
                                .collect::<Vec<_>>()}
                        </div>
                    </section>
                </Show>

                <section class="takeout">
                    <h2 class="note-title">"Take it out"</h2>
                    <Show
                        when=move || has_files
                        fallback=move || {
                            view! {
                                <p class="empty-note">
                                    "No local copies yet — the mirror has not pulled files for this book."
                                </p>
                            }
                        }
                    >
                        <div class="offers">
                            {files
                                .clone()
                                .into_iter()
                                .map(|f| view! { <FileCard id=book_id f=f /> })
                                .collect::<Vec<_>>()}
                        </div>
                    </Show>
                </section>
            </div>
        </article>
    }
}

#[component]
fn AuthorLine(a: AuthorBio) -> impl IntoView {
    let years = match (a.birth, a.death) {
        (Some(b), Some(d)) => format!(" {b}–{d}"),
        (Some(b), None) => format!(" {b}–"),
        (None, Some(d)) => format!(" ?–{d}"),
        (None, None) => String::new(),
    };
    let wiki = a.wikipedia.clone();
    view! {
        <span class="author">
            {a.name.clone()}
            <span class="author-years">{years}</span>
            {match wiki.as_deref() {
                Some(w) => view! {
                    <a class="author-wiki" href=w.to_string() target="_blank" rel="noopener" title="Wikipedia">
                        "↗"
                    </a>
                }
                    .into_any(),
                None => ().into_any(),
            }}
        </span>
    }
}

#[component]
fn Fact(label: &'static str, value: Option<String>) -> impl IntoView {
    view! {
        <div class="fact">
            <dt>{label}</dt>
            <dd>{value.unwrap_or_else(|| "—".into())}</dd>
        </div>
    }
}

#[component]
fn FileCard(id: i64, f: FileOffer) -> impl IntoView {
    let href = format!("/api/books/{id}/files/{}", f.format);
    let open_href = format!("{href}?disposition=inline");
    let stamp = f.extension.to_uppercase();
    let label = f.label.clone();
    let size = human_bytes(f.bytes);
    let download_name = format!("pg{id}.{}", f.extension);
    let is_txt = f.format == "txt";
    let cloth = cloth_style(&f.format);
    view! {
        <div class="offer" style=cloth.clone()>
            <div class="offer-stamp">{stamp.clone()}</div>
            <div class="offer-body">
                <p class="offer-label">{label.clone()}</p>
                <p class="offer-size">{size.clone()}</p>
            </div>
            <div class="offer-actions">
                <a class="offer-btn" href=href.clone() download=download_name.clone()>
                    "Download"
                </a>
                <Show when=move || is_txt>
                    <a class="offer-btn ghost" href=open_href.clone()>"Read"</a>
                </Show>
            </div>
        </div>
    }
}
