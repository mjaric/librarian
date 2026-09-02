//! librarian-web — the web shell for the bookshelf catalog.
//!
//! Serves the JSON API (`/api`), the browser bundle from `static/` (built by
//! `cargo run -p xtask -- dist`), and covers/files streamed straight out of
//! the local mirror. Read-only: it never touches the DB schema, so it can
//! run alongside the `librarian` daemon against the same Postgres.

mod api;
mod config;

use anyhow::Context;
use axum::extract::Request;
use axum::http::{HeaderValue, header::CACHE_CONTROL};
use axum::middleware::Next;
use axum::response::Response;
use bookshelf_core::StorePostgres;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::services::{ServeDir, ServeFile};

use crate::api::AppState;
use crate::config::Config;

#[derive(Parser)]
#[command(
    name = "librarian-web",
    about = "Web shell for the bookshelf catalog (read-only)",
    version
)]
struct Cli {
    /// Config path; defaults to $BOOKSHELF_CONFIG, then ./librarian.toml
    #[arg(short, long)]
    config: Option<PathBuf>,
    /// Override the listen address (default from config, else 127.0.0.1:8787)
    #[arg(long)]
    bind: Option<SocketAddr>,
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "librarian_web=info,tower_http=warn".into()),
        )
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl-c handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutting down");
}

/// Force revalidation of every static response. Bundle filenames are stable
/// (no content hash), so `no-cache` is the only safe policy: the browser keeps
/// its copy but must check ETag/Last-Modified each load — a miss is a cheap
/// 304, a hit is fresh bytes, and a stale heuristic-fresh copy is impossible.
async fn no_cache(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    res.headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    res
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let cfg = Config::load(cli.config.as_deref())?;
    let bind = cli.bind.unwrap_or(cfg.bind);

    let store = Arc::new(
        StorePostgres::connect(&cfg.database_url)
            .await
            .with_context(
                || "connecting to postgres (set BOOKSHELF_DATABASE_URL or `database_url`)",
            )?,
    );

    // Canonicalize once: every streamed file must resolve under this root.
    let library_root = tokio::fs::canonicalize(&cfg.library_dir)
        .await
        .with_context(|| format!("library_dir {} not found", cfg.library_dir.display()))?;

    // Resolve the UI bundle: explicit `static_dir` wins; else try ./static
    // (next to the binary / cwd) and the compile-time crate dir (repo runs).
    let mut static_dir = cfg.static_dir.clone();
    if !static_dir.join("index.html").is_file() {
        let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
        if fallback.join("index.html").is_file() {
            static_dir = fallback;
        }
    }
    if !static_dir.join("index.html").is_file() {
        tracing::warn!(
            "no UI bundle in {} — build it with `cargo run -p xtask -- dist`",
            static_dir.display()
        );
    }

    let state = AppState {
        store,
        library_root,
    };
    let app = axum::Router::new()
        // Static assets first; anything else (SPA history routes like
        // /books/1342) falls back to index.html with a 200 — `fallback`,
        // not `not_found_service`, which would keep the 404 status.
        .fallback_service(
            ServeDir::new(&static_dir)
                .append_index_html_on_directories(true)
                .fallback(ServeFile::new(static_dir.join("index.html"))),
        )
        // `layer` only wraps what was added before it, so `/api` (nested
        // below) keeps its responses untouched.
        .layer(axum::middleware::from_fn(no_cache))
        .nest("/api", api::router())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!(
        "librarian-web listening on http://{bind}/ (ui: {})",
        static_dir.display()
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serving")?;
    Ok(())
}
