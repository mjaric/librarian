//! CSR data loading: keyed fetch into a signal.
//!
//! leptos `Resource` demands `Send` futures; gloo-net futures are
//! wasm-local, so loads are driven by `Effect` + `spawn_local` with an
//! epoch counter that discards stale responses.

use leptos::prelude::*;

/// Whenever `key()` changes, run `fetch(key)` and store the result in `out`.
/// `out` is cleared while a load is in flight; a newer key always wins.
pub fn load<T, K, F, Fut>(
    key: F,
    fetch: impl Fn(K) -> Fut + Clone + 'static,
    out: RwSignal<Option<Result<T, String>>>,
) where
    T: Send + Sync + 'static,
    K: Clone + PartialEq + 'static,
    F: Fn() -> K + 'static,
    Fut: Future<Output = Result<T, String>> + 'static,
{
    let epoch = RwSignal::new(0usize);
    Effect::new(move |_| {
        let k = key();
        let current = epoch.get_untracked() + 1;
        epoch.set(current);
        out.set(None);
        let fetch = fetch.clone();
        leptos::task::spawn_local(async move {
            let res = fetch(k).await;
            if epoch.get_untracked() == current {
                out.set(Some(res));
            }
        });
    });
}

/// Debounce `raw` into `settled`: the last write wins after `ms` of quiet.
/// Late timers are discarded via the epoch instead of cancellation (gloo
/// timers are not `Send`, so they cannot live inside a signal).
pub fn debounce(raw: RwSignal<String>, settled: RwSignal<String>, ms: u32) {
    let epoch = RwSignal::new(0usize);
    Effect::new(move |_| {
        let v = raw.get();
        let current = epoch.get_untracked() + 1;
        epoch.set(current);
        gloo_timers::callback::Timeout::new(ms, move || {
            if epoch.get_untracked() == current {
                settled.set(v);
            }
        })
        .forget();
    });
}
