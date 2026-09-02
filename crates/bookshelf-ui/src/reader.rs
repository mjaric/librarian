//! The reading room: opens a mirrored book file in-browser and renders it
//! as a continuous column. The `bookshelf-reader` crate does the parsing;
//! this page wires the result to the DOM —
//!
//! - each block element carries `data-chr` (its char offset into the
//!   section's normalized text) and each section a `data-sec`, so the DOM
//!   and the text model can address each other;
//! - scrolling (debounced) turns the viewport into an [`AnchorRecord`] and
//!   parks it in localStorage; on open the record is resolved back to a
//!   scroll position (fingerprint first, clamped offset second,
//!   proportional section fallback last);
// - images inside sanitized HTML are re-pointed at blob URLs created from
//   `Reader::asset`, lazily per section via IntersectionObserver — and
//   eagerly through a TOC-jump or restore target, so the layout is final
//   before the viewport moves;
//!
//! Blob URLs are revoked on cleanup; if a navigation skips the cleanup the
//! URLs simply live as long as the page, which is acceptable for a reader.
//!
//! - in-book links are claimed by one delegated click handler on the
//!   scroll container: external schemes keep the browser's default
//!   behavior; fragment and relative-path targets scroll inside the book
//!   (sanitized content keeps its relative hrefs, and the SPA router
//!   would otherwise read a book href as one of its routes);

use std::sync::{Arc, Mutex};

use bookshelf_reader::{
    AnchorRecord, BLOCK_SELECTOR, BlockSpan, Reader, Section, SourceFormat, anchor_at, resolve,
};
use gloo_timers::future::TimeoutFuture;
use leptos::prelude::*;
use leptos_router::NavigateOptions;
use leptos_router::hooks::{use_navigate, use_params_map};
use leptos_use::{use_debounce_fn, use_window_scroll};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::Closure;

use crate::api;
use crate::load::load;

/// Preference order for the format-less `/books/:id/read` route.
const READ_ORDER: [&str; 3] = ["epub.images", "html.zip", "txt"];

/// Settle window for eager image loads before a programmatic scroll: long
/// enough for a slow decode, short enough not to feel like a stall.
const SETTLE_TIMEOUT_MS: f64 = 2500.0;
/// Poll interval while waiting for that settle.
const SETTLE_POLL_MS: u32 = 120;

/// Everything the reading surface needs, one per navigation.
#[derive(Clone)]
struct Loaded {
    id: i64,
    format: String,
    title: String,
    reader: Arc<Reader>,
}

fn storage_key(id: i64, format: &str) -> String {
    format!("reader:pos:project-gutenberg:{id}:{format}")
}

/// Read the stored anchor for this book, if any.
fn read_anchor(id: i64, format: &str) -> Option<AnchorRecord> {
    let window = web_sys::window()?;
    let storage = window.local_storage().ok().flatten()?;
    let json = storage.get_item(&storage_key(id, format)).ok().flatten()?;
    serde_json::from_str(&json).ok()
}

/// Fetch the catalogue entry + the mirrored file and parse it.
async fn open_book(id: i64, format: String) -> Result<Loaded, String> {
    let fmt =
        SourceFormat::parse_key(&format).ok_or_else(|| format!("unknown format \"{format}\""))?;
    let detail = api::book(id)
        .await?
        .ok_or_else(|| format!("book {id} is not in the catalogue"))?;
    if !detail.files.iter().any(|f| f.format == format) {
        return Err(format!("\"{format}\" is not mirrored for this book"));
    }
    let resp = gloo_net::http::Request::get(&format!("/api/books/{id}/files/{format}"))
        .send()
        .await
        .map_err(|e| format!("cannot reach the server: {e}"))?;
    if !resp.ok() {
        return Err(format!(
            "the server answered {} for /api/books/{id}/files/{format}",
            resp.status()
        ));
    }
    let bytes = resp
        .binary()
        .await
        .map_err(|e| format!("bad response from the server: {e}"))?;
    let reader = Reader::open(fmt, &bytes).map_err(|e| format!("cannot open this book: {e:#}"))?;
    Ok(Loaded {
        id,
        format,
        title: detail.title,
        reader: Arc::new(reader),
    })
}

/// `/books/:id/read` — redirect to the best mirrored format.
#[component]
pub fn ReadPick() -> impl IntoView {
    let params = use_params_map();
    let id = Memo::new(move |_| params.get().get("id").and_then(|s| s.parse::<i64>().ok()));
    let navigate = use_navigate();
    let fired = StoredValue::new(false);
    let failed = RwSignal::new(false);

    Effect::new(move |_| {
        let Some(id) = id.get() else { return };
        if fired.get_value() {
            return;
        }
        fired.set_value(true);
        let navigate = navigate.clone();
        leptos::task::spawn_local(async move {
            let detail = api::book(id).await.ok().flatten();
            let best = detail.and_then(|d| {
                READ_ORDER
                    .iter()
                    .find(|f| d.files.iter().any(|x| &x.format == *f))
                    .map(|f| f.to_string())
            });
            match best {
                Some(f) => navigate(&format!("/books/{id}/read/{f}"), NavigateOptions::default()),
                None => failed.set(true),
            }
        });
    });

    view! {
        <div class="empty reader-empty">
            <Show when=move || failed.get() fallback=|| {
                view! { <p class="loading">"Choosing a format…"</p> }.into_any()
            }>
                <p class="empty-title">"Nothing to read yet."</p>
                <p class="empty-note">"No readable file of this book is mirrored."</p>
                <p class="empty-note">
                    <a href="/">"Back to the reading desk."</a>
                </p>
            </Show>
        </div>
    }
}

/// `/books/:id/read/:format` — the reading surface itself.
#[component]
pub fn ReadView() -> impl IntoView {
    let params = use_params_map();
    let key = Memo::new(move |_| {
        let id = params.get().get("id").and_then(|s| s.parse::<i64>().ok())?;
        let format = params.get().get("format")?;
        Some((id, format))
    });
    let loaded = RwSignal::new(None::<Result<Loaded, String>>);
    load(
        move || key.get(),
        |key| async move {
            match key {
                Some((id, format)) => open_book(id, format).await,
                None => Err("no such page".into()),
            }
        },
        loaded,
    );

    view! {
        <div class="reader">
            {move || match loaded.get() {
                None => view! { <p class="loading">"Opening the book…"</p> }.into_any(),
                Some(Err(e)) => view! {
                    <div class="empty">
                        <p class="empty-title">"The book won't open."</p>
                        <p class="empty-note">{e}</p>
                        <p class="empty-note">
                            <a href="/">"Back to the reading desk."</a>
                        </p>
                    </div>
                }
                    .into_any(),
                Some(Ok(l)) => view! { <ReaderBook loaded=l /> }.into_any(),
            }}
        </div>
    }
}

#[component]
fn ReaderBook(loaded: Loaded) -> impl IntoView {
    let reader = loaded.reader.clone();
    let armed = RwSignal::new(false);
    let created: Arc<Mutex<Vec<String>>> = Arc::default();
    let observers: Arc<Mutex<Vec<web_sys::IntersectionObserver>>> = Arc::default();

    // Debounced position save: every scroll tick schedules one save; the
    // last one wins after 300 ms of quiet. Disabled until the initial
    // restore ran, so the first scroll event cannot overwrite the record
    // we are about to restore from.
    let (_, y) = use_window_scroll();
    {
        let reader = reader.clone();
        let (id, format) = (loaded.id, loaded.format.clone());
        let save = use_debounce_fn(
            move || {
                if !armed.get_untracked() {
                    return;
                }
                save_position(&reader, id, &format);
            },
            300.0,
        );
        Effect::new(move |_| {
            let _ = y.get();
            save();
        });
    }

    // One pass after the sections are in the DOM: tag blocks with
    // data-chr, set up lazy image loading, restore the saved position,
    // then arm saving. `armed` is the (untracked) one-shot guard.
    {
        let reader = reader.clone();
        let created = created.clone();
        let observers = observers.clone();
        // Capture the stored record BEFORE any save can overwrite it.
        let record = StoredValue::new(read_anchor(loaded.id, &loaded.format));
        Effect::new(move |_| {
            if armed.get_untracked() {
                return;
            }
            for (idx, sec) in reader.sections().iter().enumerate() {
                if sec.html.is_empty() {
                    continue; // txt sections are tagged inline in the view
                }
                let Some(body) = document().get_element_by_id(&format!("reader-body-{idx}")) else {
                    continue;
                };
                tag_blocks(&body, &sec.blocks);
                if let Some(o) = watch_images(body, reader.clone(), created.clone()) {
                    observers.lock().expect("observer lock").push(o);
                }
            }
            sync_head_vars();
            if let Some(rec) = record.get_value() {
                // Restore scrolls once, after every image above the anchor
                // has settled; only then may saving re-arm.
                let reader = reader.clone();
                let created = created.clone();
                leptos::task::spawn_local(async move {
                    restore_position(&reader, &created, &rec).await;
                    armed.set(true);
                });
            } else {
                armed.set(true);
            }
        });
    }

    on_cleanup({
        let observers = observers.clone();
        let created = created.clone();
        move || {
            for o in observers.lock().expect("observer lock").iter() {
                o.disconnect();
            }
            for url in created.lock().expect("blob url lock").iter() {
                let _ = web_sys::Url::revoke_object_url(url);
            }
        }
    });

    // TOC rows mapped onto section indexes, one row per section.
    let mut toc_rows: Vec<(usize, String)> = Vec::new();
    for entry in reader.toc() {
        if let Some(i) = reader
            .sections()
            .iter()
            .position(|s| s.id == entry.section_id)
        {
            if toc_rows.iter().any(|(seen, _)| *seen == i) {
                continue;
            }
            toc_rows.push((i, entry.title.clone().unwrap_or_else(|| "Untitled".into())));
        }
    }
    let has_toc = !toc_rows.is_empty();

    // Delegated in-book link handling: one listener for every anchor in
    // the rendered book. See the module docs — external hrefs are left
    // to the browser, everything else scrolls without touching the
    // router.
    let link_click = {
        let reader = reader.clone();
        let created = created.clone();
        move |ev: leptos::ev::MouseEvent| {
            let Some(anchor) = ev
                .target()
                .and_then(|t| t.dyn_into::<web_sys::Element>().ok())
                .and_then(|t| t.closest("a").ok().flatten())
            else {
                return;
            };
            let Some(href) = anchor.get_attribute("href") else {
                return;
            };
            if is_external_href(&href) {
                return; // default browser behavior, untouched
            }
            // Everything below is ours: a book href must never reach the
            // router, even when its target cannot be found.
            ev.prevent_default();
            let (path, frag) = match href.split_once('#') {
                Some((p, f)) => (p, Some(f)),
                None => (href.as_str(), None),
            };
            let reader = reader.clone();
            let created = created.clone();
            if path.is_empty() {
                // Fragment-only: same document, target anywhere in the
                // rendered book; its containing section settles first.
                if let Some(target) = frag.and_then(|f| document().get_element_by_id(f)) {
                    if let Some(idx) = section_index_of(&target) {
                        leptos::task::spawn_local(async move {
                            land_on(&target, idx, &reader, &created).await;
                        });
                    }
                }
                return;
            }
            // Relative path (± fragment): resolve against the linking
            // section's own source document.
            let target_section = section_index_of(&anchor)
                .and_then(|i| reader.sections().get(i).and_then(|s| s.source.clone()))
                .and_then(|from| reader.section_index_for_path(&from, path));
            match target_section {
                Some(i) => {
                    // A fragment inside the target section wins; else the
                    // section's heading, exactly like a TOC jump.
                    let target = frag
                        .and_then(|f| fragment_in_section(f, i))
                        .or_else(|| jump_target(i));
                    if let Some(target) = target {
                        leptos::task::spawn_local(async move {
                            land_on(&target, i, &reader, &created).await;
                        });
                    }
                }
                None => {
                    web_sys::console::warn_1(&format!("book link target not found: {href}").into());
                }
            }
        }
    };

    let toc_items = toc_rows
        .into_iter()
        .map(|(i, t)| {
            let href = format!("#reader-sec-{i}");
            let reader = reader.clone();
            let created = created.clone();
            let jump = move |ev: leptos::ev::MouseEvent| {
                ev.prevent_default();
                // Aim at the chapter heading, not the section box: a
                // section may open with pages of front matter before its
                // title. Settle every image above it first, so the late
                // blob loads cannot push the chapter out from under the
                // viewport, then scroll.
                let Some(target) = jump_target(i) else {
                    return;
                };
                let reader = reader.clone();
                let created = created.clone();
                leptos::task::spawn_local(async move {
                    settle_images(&bodies_through(i), &reader, &created).await;
                    sync_head_vars();
                    target.scroll_into_view();
                });
                // Fold the dropdown away once a chapter is chosen; the
                // list is absolutely positioned, so closing cannot shift
                // the layout the scroll just aimed at.
                if let Some(details) = ev
                    .target()
                    .and_then(|t| t.dyn_into::<web_sys::Element>().ok())
                    .and_then(|t| t.closest("details").ok().flatten())
                {
                    let _ = details.remove_attribute("open");
                }
            };
            view! {
                <li>
                    <a href=href on:click=jump>{t}</a>
                </li>
            }
        })
        .collect_view();

    let toc_el = if has_toc {
        view! {
            <details class="reader-toc">
                <summary>"Contents"</summary>
                <ul>{toc_items}</ul>
            </details>
        }
        .into_any()
    } else {
        ().into_any()
    };

    let sections = reader.sections().to_vec();
    view! {
        <div class="reader-head">
            <div class="reader-head-in">
                <a class="reader-back" href={format!("/books/{}", loaded.id)}>
                    "← " {loaded.title.clone()}
                </a>
                {toc_el}
            </div>
        </div>
        <div class="reader-scroll" on:click=link_click>
            {sections
                .into_iter()
                .enumerate()
                .map(|(i, s)| view! { <SectionView index=i sec=s /> })
                .collect_view()}
        </div>
    }
}

#[component]
fn SectionView(index: usize, sec: Section) -> impl IntoView {
    let sec_id = format!("reader-sec-{index}");
    let body_id = format!("reader-body-{index}");
    let sec_attr = index.to_string();

    if sec.html.is_empty() {
        // txt: render the normalized text straight from the blocks.
        let paragraphs = sec
            .blocks
            .iter()
            .filter(|b| b.end > b.start)
            .map(|b| {
                let text: String = sec
                    .text
                    .chars()
                    .skip(b.start)
                    .take(b.end - b.start)
                    .collect();
                let chr = b.start.to_string();
                view! { <p class="reader-p" data-chr=chr>{text}</p> }
            })
            .collect_view();
        view! {
            <section class="reader-section" id=sec_id data-sec=sec_attr>
                <div class="reader-section-body" id=body_id>
                    {paragraphs}
                </div>
            </section>
        }
        .into_any()
    } else {
        view! {
            <section class="reader-section" id=sec_id data-sec=sec_attr>
                <div class="reader-section-body" id=body_id inner_html=sec.html></div>
            </section>
        }
        .into_any()
    }
}

/// Pair rendered block elements with `Section.blocks` by document order —
/// the walker emits one span per block element for exactly this.
fn tag_blocks(container: &web_sys::Element, blocks: &[BlockSpan]) {
    let Ok(nodes) = container.query_selector_all(BLOCK_SELECTOR) else {
        return;
    };
    let n = nodes.length().min(blocks.len() as u32);
    for i in 0..n {
        let Some(el) = nodes.get(i) else { continue };
        let el: web_sys::Element = el.unchecked_into();
        let b = blocks[i as usize];
        if b.end > b.start {
            let _ = el.set_attribute("data-chr", &b.start.to_string());
        }
    }
}

/// Observe a section; on first intersection re-point its images at blob
/// URLs. Returns the observer so the caller can disconnect on cleanup.
fn watch_images(
    container: web_sys::Element,
    reader: Arc<Reader>,
    created: Arc<Mutex<Vec<String>>>,
) -> Option<web_sys::IntersectionObserver> {
    let observed = container.clone();
    let cb = Closure::wrap(Box::new(
        move |entries: js_sys::Array, observer: web_sys::IntersectionObserver| {
            let hit = entries
                .iter()
                .any(|e| web_sys::IntersectionObserverEntry::from(e).is_intersecting());
            if hit {
                rewrite_images(&observed, &reader, &created);
                observer.unobserve(&observed);
            }
        },
    )
        as Box<dyn FnMut(js_sys::Array, web_sys::IntersectionObserver)>);
    let observer = web_sys::IntersectionObserver::new(cb.into_js_value().unchecked_ref()).ok()?;
    observer.observe(&container);
    Some(observer)
}

fn rewrite_images(container: &web_sys::Element, reader: &Reader, created: &Mutex<Vec<String>>) {
    let Ok(imgs) = container.query_selector_all("img[src]") else {
        return;
    };
    for i in 0..imgs.length() {
        let Some(img) = imgs.get(i) else { continue };
        let img: web_sys::Element = img.unchecked_into();
        let Some(src) = img.get_attribute("src") else {
            continue;
        };
        if src.starts_with("data:")
            || src.starts_with("http:")
            || src.starts_with("https:")
            || src.starts_with("blob:")
        {
            continue;
        }
        let bytes = reader
            .asset(&src)
            .or_else(|| src.rsplit('/').next().and_then(|base| reader.asset(base)));
        let Some(bytes) = bytes else { continue };
        let arr = js_sys::Array::new();
        arr.push(&js_sys::Uint8Array::from(bytes));
        let Ok(blob) = web_sys::Blob::new_with_u8_array_sequence(&arr) else {
            continue;
        };
        if let Ok(url) = web_sys::Url::create_object_url_with_blob(&blob) {
            let _ = img.set_attribute("src", &url);
            created.lock().expect("blob url lock").push(url);
        }
    }
}

/// Publish the pinned bars' measured heights (site topbar + reading
/// toolbar) as CSS custom properties on the reader root, so
/// `scroll-margin-top` in the stylesheet and any JS scroll offset share
/// one number. Returns the combined clearance.
fn sync_head_vars() -> f64 {
    let document = leptos::prelude::document();
    let mut clearance = 0.0;
    let mut style = String::new();
    for (name, sel) in [
        ("--topbar-h", ".topbar"),
        ("--reader-head-h", ".reader-head"),
    ] {
        if let Some(el) = document.query_selector(sel).ok().flatten() {
            let h = el.get_bounding_client_rect().height().ceil();
            clearance += h;
            style.push_str(&format!("{name}:{h}px;"));
        }
    }
    if let Some(root) = document.query_selector(".reader").ok().flatten() {
        let _ = root.set_attribute("style", &style);
    }
    clearance
}

/// Section bodies from the top of the book through `idx` — everything
/// whose images can push a target at or after them out of place.
fn bodies_through(idx: usize) -> Vec<web_sys::Element> {
    (0..=idx)
        .filter_map(|i| document().get_element_by_id(&format!("reader-body-{i}")))
        .collect()
}

/// The element a TOC entry aims at: the section's first heading — a
/// section may open with pages of front matter before its title — or the
/// section itself when it has none.
fn jump_target(idx: usize) -> Option<web_sys::Element> {
    let sec = document().get_element_by_id(&format!("reader-sec-{idx}"))?;
    sec.query_selector("h1,h2,h3,h4,h5,h6")
        .ok()
        .flatten()
        .or(Some(sec))
}

/// The `[data-sec]` section index an element inside the rendered book
/// belongs to — the origin of a clicked link, or the owner of a fragment
/// target.
fn section_index_of(el: &web_sys::Element) -> Option<usize> {
    el.closest("[data-sec]")
        .ok()
        .flatten()
        .and_then(|s| s.get_attribute("data-sec"))
        .and_then(|v| v.parse().ok())
}

/// Links that leave the book and keep the browser's default behavior,
/// untouched: absolute http(s), mailto, data URIs, protocol-relative.
fn is_external_href(href: &str) -> bool {
    href.starts_with("http://")
        || href.starts_with("https://")
        || href.starts_with("mailto:")
        || href.starts_with("data:")
        || href.starts_with("//")
}

/// The element a `#fragment` inside section `idx` aims at. Ids are only
/// unique per source document — the first element with that id in the
/// rendered book may sit in another section — so the owner is verified.
fn fragment_in_section(frag: &str, idx: usize) -> Option<web_sys::Element> {
    let el = document().get_element_by_id(frag)?;
    section_index_of(&el).filter(|&owner| owner == idx)?;
    Some(el)
}

/// Land on an in-book link target the way the TOC jump and the position
/// restore land: settle every image through the containing section first
/// (a late blob load must not shift the layout mid-scroll), re-measure
/// the pinned bars, then aim at the element with that clearance — the
/// same offset the stylesheet's `scroll-margin-top` computes, but in JS
/// so arbitrary id targets clear the bars too, not just sections and
/// headings.
async fn land_on(
    target: &web_sys::Element,
    idx: usize,
    reader: &Reader,
    created: &Mutex<Vec<String>>,
) {
    settle_images(&bodies_through(idx), reader, created).await;
    let clearance = sync_head_vars();
    if let Some(window) = web_sys::window() {
        let rect = target.get_bounding_client_rect();
        let abs = rect.top() + window.scroll_y().unwrap_or(0.0);
        scroll_to(&window, (abs - clearance).max(0.0));
    }
}

/// Resolve every image in `containers` right now (the lazy path waits for
/// intersection), then wait until they have all fired load/error and the
/// document height has stopped moving — or until [`SETTLE_TIMEOUT_MS`],
/// so one stubborn image cannot freeze the jump forever.
async fn settle_images(
    containers: &[web_sys::Element],
    reader: &Reader,
    created: &Mutex<Vec<String>>,
) {
    for container in containers {
        rewrite_images(container, reader, created);
    }
    let Some(root) = leptos::prelude::document().document_element() else {
        return;
    };
    let deadline = js_sys::Date::now() + SETTLE_TIMEOUT_MS;
    let mut height = root.scroll_height();
    while pending_images(containers) > 0 || root.scroll_height() != height {
        if js_sys::Date::now() >= deadline {
            break;
        }
        TimeoutFuture::new(SETTLE_POLL_MS).await;
        height = root.scroll_height();
    }
}

/// Images in `containers` whose blob (or original) source has not
/// finished loading — each one still owes the layout a height change.
fn pending_images(containers: &[web_sys::Element]) -> u32 {
    let mut pending = 0;
    for container in containers {
        if let Ok(imgs) = container.query_selector_all("img") {
            for i in 0..imgs.length() {
                if let Some(img) = imgs.get(i) {
                    let img: web_sys::HtmlImageElement = img.unchecked_into();
                    if !img.complete() {
                        pending += 1;
                    }
                }
            }
        }
    }
    pending
}

/// Viewport → anchor: the topmost block whose bottom edge is below the
/// viewport top, with an intra-block estimate from how far it scrolled
/// past. `None` when nothing is rendered yet.
fn current_position(reader: &Reader) -> Option<AnchorRecord> {
    let document = leptos::prelude::document();
    let nodes = document.query_selector_all("[data-chr]").ok()?;
    for i in 0..nodes.length() {
        let Some(el) = nodes.get(i) else { continue };
        let el: web_sys::Element = el.unchecked_into();
        let rect = el.get_bounding_client_rect();
        if rect.bottom() <= 0.0 {
            continue; // entirely above the viewport
        }
        let chr: usize = el.get_attribute("data-chr")?.parse().ok()?;
        let sec_idx: usize = el
            .closest("[data-sec]")
            .ok()
            .flatten()
            .and_then(|p| p.get_attribute("data-sec"))
            .and_then(|v| v.parse().ok())?;
        let sec = reader.sections().get(sec_idx)?;
        let span = sec.blocks.iter().find(|b| b.start == chr)?;
        let height = rect.height().max(1.0);
        let into = (-rect.top()).clamp(0.0, height);
        let off = chr + (into / height * (span.end - span.start) as f64) as usize;
        return Some(anchor_at(sec, off.min(sec.text.len_chars())));
    }
    None
}

fn save_position(reader: &Reader, id: i64, format: &str) {
    let Some(record) = current_position(reader) else {
        return;
    };
    let Ok(json) = serde_json::to_string(&record) else {
        return;
    };
    if let Some(storage) = web_sys::window().and_then(|w| w.local_storage().ok().flatten()) {
        let _ = storage.set_item(&storage_key(id, format), &json);
    }
}

/// Anchor → viewport: fingerprint resolve first, clamped offset second;
/// when the record's section no longer exists, fall back proportionally
/// to the nearest section start. Every image through the target section
/// settles before the scroll, so the layout cannot shift under it; the
/// clearance is the live-measured height of the pinned bars.
async fn restore_position(reader: &Reader, created: &Mutex<Vec<String>>, record: &AnchorRecord) {
    let Some(window) = web_sys::window() else {
        return;
    };
    let document = leptos::prelude::document();

    if let Some((doc, off)) = resolve(reader, record) {
        if let Some(idx) = reader.sections().iter().position(|s| s.id == doc) {
            settle_images(&bodies_through(idx), reader, created).await;
            let clearance = sync_head_vars();
            let sec = &reader.sections()[idx];
            // The rendered block holding `off`: the last tagged block at or
            // before the offset, in document order. Derived from the DOM,
            // so it cannot miss the way span bookkeeping can.
            let target = tagged_block_before(idx, off);
            if let Some(el) = target {
                let start = el
                    .get_attribute("data-chr")
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(0);
                let end = sec
                    .blocks
                    .iter()
                    .find(|b| b.start == start)
                    .map_or(start, |b| b.end);
                let rect = el.get_bounding_client_rect();
                let frac = if end > start {
                    ((off - start) as f64 / (end - start) as f64).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                let abs = rect.top() + window.scroll_y().unwrap_or(0.0);
                scroll_to(&window, (abs + frac * rect.height() - clearance).max(0.0));
            } else if let Some(el) = document.get_element_by_id(&format!("reader-sec-{idx}")) {
                let rect = el.get_bounding_client_rect();
                let abs = rect.top() + window.scroll_y().unwrap_or(0.0);
                scroll_to(&window, (abs - clearance).max(0.0));
            }
            return;
        }
    }

    // Proportional fallback: the section is gone (format switched, say) —
    // walk cumulative offsets to the section holding the recorded offset.
    let mut cum = 0usize;
    let last = reader.sections().len().saturating_sub(1);
    for (idx, sec) in reader.sections().iter().enumerate() {
        if record.start < cum + sec.text.len_chars() || idx == last {
            settle_images(&bodies_through(idx), reader, created).await;
            let clearance = sync_head_vars();
            if let Some(el) = document.get_element_by_id(&format!("reader-sec-{idx}")) {
                let rect = el.get_bounding_client_rect();
                let abs = rect.top() + window.scroll_y().unwrap_or(0.0);
                scroll_to(&window, (abs - clearance).max(0.0));
            }
            return;
        }
        cum += sec.text.len_chars();
    }
}

/// The rendered block holding `off`: the last `[data-chr]` element at or
/// before the offset, in document order (tags ascend with the text).
fn tagged_block_before(idx: usize, off: usize) -> Option<web_sys::Element> {
    let nodes = leptos::prelude::document()
        .query_selector_all(&format!("#reader-sec-{idx} [data-chr]"))
        .ok()?;
    let mut best = None;
    for i in 0..nodes.length() {
        let Some(el) = nodes.get(i) else { continue };
        let el: web_sys::Element = el.unchecked_into();
        let Some(chr) = el
            .get_attribute("data-chr")
            .and_then(|v| v.parse::<usize>().ok())
        else {
            continue;
        };
        if chr > off {
            break;
        }
        best = Some(el);
    }
    best
}

fn scroll_to(window: &web_sys::Window, top: f64) {
    let opts = web_sys::ScrollToOptions::new();
    opts.set_top(top);
    window.scroll_to_with_scroll_to_options(&opts);
}
