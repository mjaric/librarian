//! `librarian-web` configuration.
//!
//! Reads the same file family as the daemon (`--config` → `$BOOKSHELF_CONFIG`
//! → `./librarian.toml`) but keeps only the keys it needs: `database_url`,
//! `library_dir`, plus `bind` and `static_dir`. Unknown keys are ignored on
//! purpose so one config file can drive both the daemon and the web shell.
//! `BOOKSHELF_DATABASE_URL` always overrides the file, as everywhere else.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub library_dir: PathBuf,
    pub bind: SocketAddr,
    pub static_dir: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
struct ConfigFile {
    database_url: Option<String>,
    library_dir: Option<String>,
    bind: Option<String>,
    static_dir: Option<String>,
}

impl Config {
    pub fn load(explicit: Option<&Path>) -> anyhow::Result<Self> {
        let path: Option<PathBuf> = match explicit {
            Some(p) => Some(p.to_path_buf()),
            None => std::env::var("BOOKSHELF_CONFIG")
                .ok()
                .map(PathBuf::from)
                .or_else(|| {
                    let local = PathBuf::from("librarian.toml");
                    local.is_file().then_some(local)
                }),
        };
        // Deliberately no deny_unknown_fields: the daemon's file has more keys.
        let file: ConfigFile = match &path {
            Some(p) => {
                let raw = std::fs::read_to_string(p)
                    .with_context(|| format!("reading config {}", p.display()))?;
                toml::from_str(&raw).with_context(|| format!("parsing config {}", p.display()))?
            }
            None => ConfigFile::default(),
        };

        let database_url = std::env::var("BOOKSHELF_DATABASE_URL")
            .ok()
            .or(file.database_url)
            .unwrap_or_else(|| "postgres://bookshelf:bookshelf@localhost:5432/bookshelf".into());

        Ok(Self {
            database_url,
            library_dir: expand_tilde(file.library_dir.unwrap_or_else(|| "./library".into())),
            bind: file
                .bind
                .unwrap_or_else(|| "127.0.0.1:8787".into())
                .parse()
                .context("parsing `bind` (want host:port)")?,
            static_dir: expand_tilde(file.static_dir.unwrap_or_else(|| "./static".into())),
        })
    }
}

/// Expand a leading `~` against $HOME (mirrors the daemon's `config.rs`).
fn expand_tilde(path: String) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return Path::new(&home).join(rest);
        }
    }
    PathBuf::from(path)
}
