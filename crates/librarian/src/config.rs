//! `librarian.toml` configuration. Every key is optional with code defaults;
//! search order: `--config PATH` → `$BOOKSHELF_CONFIG` → `./librarian.toml`.
//! Keys apply to the single registered provider (`project-gutenberg`).
//! `BOOKSHELF_DATABASE_URL` overrides the file (keeps creds out of it).

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;

use crate::gutenberg_org::rdf::Format;
use crate::supervisor::{LauncherKind, OnStop, SupervisorCfg};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriageMode {
    Rules,
    Agent,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub library_dir: PathBuf,
    pub rsync_host: String,
    pub rsync_module: String,
    pub download_host: String,
    /// Ordered subset of the 4 format keys we mirror.
    pub formats: Vec<Format>,
    pub max_parallel_downloads: usize,
    pub request_interval_ms: u64,
    pub timeout_secs: u64,
    pub max_total_attempts: u32,
    pub circuit_breaker: u32,
    pub full_sync_interval_days: u64,
    pub feed_check_days: u64,
    pub backfill_on_start: bool,
    pub contact_email: String,
    pub triage: TriageMode,
    pub agent_provider: String,
    pub agent_model: String,
    /// `[observability] otlp_endpoint` — OTLP/HTTP endpoint for metrics AND
    /// "http://127.0.0.1:4318/" from the host or
    /// "http://host.docker.internal:4318/" from a container (collector on
    /// the host network). Absent → OpenTelemetry is fully disabled: no SDK,
    /// no exporter, no network traffic. Opt-in by design.
    pub otlp_endpoint: Option<String>,
    /// `[supervisor]` — detached rsync supervision: stop behaviour, poll
    /// cadence, stall threshold and retry-ladder budget.
    pub supervisor: SupervisorCfg,
    /// `[supervisor] launcher`, resolved to a kind at load.
    pub supervisor_launcher: LauncherKind,
    /// `[supervisor] docker_image` — required iff
    /// `launcher = "docker"`; the image must contain rsync.
    pub docker_image: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ConfigFile {
    database_url: Option<String>,
    library_dir: Option<String>,
    rsync_host: Option<String>,
    rsync_module: Option<String>,
    download_host: Option<String>,
    formats: Option<Vec<String>>,
    max_parallel_downloads: Option<usize>,
    request_interval_ms: Option<u64>,
    timeout_secs: Option<u64>,
    max_total_attempts: Option<u32>,
    circuit_breaker: Option<u32>,
    full_sync_interval_days: Option<u64>,
    feed_check_days: Option<u64>,
    backfill_on_start: Option<bool>,
    contact_email: Option<String>,
    triage: Option<String>,
    agent: Option<AgentFile>,
    observability: Option<ObservabilityFile>,
    supervisor: Option<SupervisorFile>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ObservabilityFile {
    otlp_endpoint: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AgentFile {
    provider: Option<String>,
    model: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SupervisorFile {
    on_daemon_stop: Option<String>,
    launcher: Option<String>,
    poll_secs: Option<u64>,
    progress_stall_secs: Option<u64>,
    max_attempts: Option<u32>,
    /// Accepted for forward-compatibility with the docker workstream.
    docker_image: Option<String>,
}

/// Expand a leading `~` against $HOME (std does not do it by itself).
fn expand_tilde(path: String) -> PathBuf {
    if path == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home);
        }
    } else if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}

impl Config {
    /// `explicit` is the `--config` path. Missing files in the search chain
    /// are fine (all defaults); an explicit path that cannot be read is not.
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
        let file: ConfigFile = match &path {
            Some(p) => {
                let raw = std::fs::read_to_string(p)
                    .with_context(|| format!("reading config {}", p.display()))?;
                toml::from_str(&raw).with_context(|| format!("parsing config {}", p.display()))?
            }
            None => ConfigFile::default(),
        };

        let mut formats = Vec::new();
        for key in file.formats.unwrap_or_else(|| {
            ["txt", "epub.images", "html.zip", "cover"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        }) {
            let f = Format::parse_key(&key).with_context(|| {
                format!(
                    "unknown format key {key:?} in config (want txt|epub.images|html.zip|cover)"
                )
            })?;
            if !formats.contains(&f) {
                formats.push(f);
            }
        }
        anyhow::ensure!(
            !formats.is_empty(),
            "config `formats` must select at least one format"
        );

        let database_url = std::env::var("BOOKSHELF_DATABASE_URL")
            .ok()
            .or(file.database_url)
            .unwrap_or_else(|| "postgres://bookshelf:bookshelf@localhost:5432/bookshelf".into());

        let sup = file.supervisor.unwrap_or_default();
        let on_daemon_stop = match sup.on_daemon_stop.as_deref() {
            None | Some("detach") => OnStop::Detach,
            Some("kill") => OnStop::Kill,
            Some(other) => {
                anyhow::bail!("unknown supervisor on_daemon_stop {other:?} (want detach|kill)")
            }
        };
        let supervisor_launcher = match sup.launcher.as_deref() {
            None | Some("process") => LauncherKind::Process,
            Some("docker") => LauncherKind::Docker,
            Some(other) => {
                anyhow::bail!("unknown supervisor launcher {other:?} (want process|docker)")
            }
        };
        let docker_image = sup.docker_image;
        let supervisor = SupervisorCfg {
            on_daemon_stop,
            poll: Duration::from_secs(sup.poll_secs.unwrap_or(10)),
            progress_stall: Duration::from_secs(sup.progress_stall_secs.unwrap_or(30 * 60)),
            max_attempts: sup.max_attempts.unwrap_or(4),
        };

        let agent = file.agent.unwrap_or_default();
        Ok(Self {
            database_url,
            library_dir: expand_tilde(file.library_dir.unwrap_or_else(|| "./library".into())),
            rsync_host: file
                .rsync_host
                .unwrap_or_else(|| "gutenberg.pglaf.org".into()),
            rsync_module: file.rsync_module.unwrap_or_else(|| "gutenberg-epub".into()),
            download_host: file
                .download_host
                .unwrap_or_else(|| "https://www.gutenberg.org".into()),
            formats,
            max_parallel_downloads: file.max_parallel_downloads.unwrap_or(4),
            request_interval_ms: file.request_interval_ms.unwrap_or(2000),
            timeout_secs: file.timeout_secs.unwrap_or(60),
            max_total_attempts: file.max_total_attempts.unwrap_or(12),
            circuit_breaker: file.circuit_breaker.unwrap_or(15),
            full_sync_interval_days: file.full_sync_interval_days.unwrap_or(7),
            feed_check_days: file.feed_check_days.unwrap_or(1),
            backfill_on_start: file.backfill_on_start.unwrap_or(true),
            contact_email: file.contact_email.unwrap_or_default(),
            triage: match file.triage.as_deref() {
                None | Some("rules") => TriageMode::Rules,
                Some("agent") => TriageMode::Agent,
                Some(other) => anyhow::bail!("unknown triage mode {other:?} (want rules|agent)"),
            },
            agent_provider: agent.provider.unwrap_or_else(|| "zai".into()),
            agent_model: agent.model.unwrap_or_else(|| "glm-5.3".into()),
            otlp_endpoint: file.observability.and_then(|o| o.otlp_endpoint),
            supervisor,
            supervisor_launcher,
            docker_image,
        })
    }

    pub fn user_agent(&self) -> String {
        let mut ua = format!("librarian/0.1 (bookshelf backend)");
        if !self.contact_email.is_empty() {
            ua.push_str(&format!("; +{}", self.contact_email));
        }
        ua
    }

    pub fn mirror_dir(&self) -> PathBuf {
        self.library_dir.join("mirror")
    }

    pub fn meta_dir(&self) -> PathBuf {
        self.library_dir.join("meta")
    }

    /// Detached-run root: `library_dir/run`. Derived — never
    /// file-settable, like `mirror_dir`/`meta_dir`.
    pub fn run_root(&self) -> PathBuf {
        self.library_dir.join("run")
    }

    pub fn events_path(&self) -> PathBuf {
        self.library_dir.join("events.jsonl")
    }
}
