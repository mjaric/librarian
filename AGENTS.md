# Repository Guidelines

Binding rules (restated from README *Contributing*; details in sections below):

- Never add a dependency younger than 2 weeks. Exact-pin (`=a.b.c`), `Cargo.lock` committed.
- Hexagonal architecture is a must, but not at the cost of maintainability — see the trait-extraction rule under *Architecture*.
- Keep `README.md` (setup, configuration, usage, verification) accurate after every change; an undocumented change isn't done.

## Project Overview

`librarian` (Rust workspace, 2 crates) mirrors the Project Gutenberg archive — politely, via their sanctioned rsync service — into PostgreSQL plus an append-only JSONL event log. Provider-based: everything Gutenberg-specific lives behind the `gutenberg_org` provider module; a second source = new module + one registry entry. Backend only; no HTTP server, no frontend.

## Architecture & Data Flow

Hexagonal-lite, two crates:

- **`crates/bookshelf-core`** — source-agnostic side. Domain model + ports (`EventSink`, `Triage`) in `src/domain.rs`; adapters in `src/adapters/`: `store_postgres.rs` (sqlx pool + embedded migrations + all queries), `event_log.rs` (append-only JSONL), `http.rs` (`PoliteClient`), `rsync.rs` (`RsyncRunner`), `triage_rules.rs` (deterministic), `triage_agent.rs` (optional LLM, feature `agent`).
- **`crates/librarian`** — the binary. `src/main.rs` (clap CLI + daemon), `config.rs`, `provider.rs` (Provider trait + registry), `queue.rs` (Postgres job queue), `gutenberg_org/` (mirror / rdf / feed / taxonomy).

Core flows:

1. **Job model**: `sync` / `repair` CLI subcommands never execute — they enqueue + `pg_notify`, then optionally block on `--wait`. Only `librarian daemon` executes: a scheduler (30 s tick) enqueues due cycles at priority 0 (`Trigger::Schedule`); CLI jobs run at priority 10; pickup uses `FOR UPDATE SKIP LOCKED`; interrupted jobs are requeued (≥3 interruptions → failed).
2. **Sync cycle**: full rsync (mtime-delta, `--delete`) or feed-driven targeted pull → `parse_itemize` → RDF ingest fills `books`/`book_files`/`categories`/`book_categories` → per-book sidecar `{library_dir}/meta/{id}.json` (`.tmp` + rename) → event appended.
3. **Repair cycle**: stride-partitioned workers over failed files, circuit breaker (15 consecutive retriable → abort pass), triage ladder in `triage_rules.rs` (1→+5 m, 2→+10 m, 3→+1 h, 4–11 defer, ≥12 fail; 429 honors Retry-After; 404/410 → skip). With `triage = "agent"`, ambiguous cases go to the LLM at ≥3 attempts — which always falls back to rules, so keyless default is fully functional.

Non-negotiable invariants (do not "fix" these):

- **Single worker = serialized cycles — that is the politeness mechanism.** No parallel cycle execution.
- **`events.jsonl` is append-only.** Never rewritten or truncated; EventLog flushes per line.
- rsync children run in their own process groups with `PR_SET_PDEATHSIG` (`adapters/rsync.rs`); shutdown is cooperative (`InterruptFlag`: TERM → 10 s → KILL). Don't spawn rsync any other way.
- All HTTP goes through `PoliteClient` (global rate limiter `request_interval_ms` + concurrency semaphore). Never fetch Gutenberg HTML pages — the mirror carries metadata.
- **Trait discipline** (documented in `domain.rs:7-10`): no ~25-method trait for one impl. `StorePostgres` is deliberately a concrete struct; extract a trait from real usage only when a second backend exists.

## Key Directories

| Path | Purpose |
| --- | --- |
| `crates/bookshelf-core/src/` | Domain, ports, adapters (see above) |
| `crates/bookshelf-core/migrations/` | `0001_init.sql` (books/book_files/categories/book_categories/sync_runs/meta), `0002_jobs.sql` (jobs + pickup index) |
| `crates/librarian/src/` | CLI/daemon, config, provider registry, queue, `gutenberg_org/` |
| `crates/*/tests/` | Integration tests + `librarian/tests/fixtures/` |
| `library/` | Default `library_dir` target — currently empty. **A default-config daemon starts a ~230 GB backfill (1–3 days). Never run disk-touching commands without an explicit small `--config`.** |
| `library-smoke/` | Gitignored, live rsync-pulled smoke fixture (~2–3 GB, server mtimes). Read-only ground truth for eyeballing; assertions never touch it. |
| `docker/` | Container config (`library_dir = /data` only) |

## Development Commands

```sh
cargo build && cargo test                    # unit + fixture tests, no DB needed
BOOKSHELF_DATABASE_URL=postgres://… cargo test   # + store/queue integration tests
cargo test -p librarian --test rdf_parse parses_pg1342_exactly   # single test
cargo run -p librarian -- status             # DB state via same URL
cargo run -p librarian -- daemon --config librarian-smoke.toml   # smoke (small, feed off, no backfill)
cargo build --release --locked -p librarian
cargo install --path crates/librarian
docker compose up -d --build                 # postgres:16 + librarian daemon
```

No CI exists — verification is the manual loop above. No rustfmt/clippy config committed; match surrounding style.

## Code Conventions & Common Patterns

- **Errors**: `anyhow` throughout; `FetchError` (in `adapters/http.rs`) carries status / Retry-After / headers / 500-byte body head so triage stays deterministic. EventLog errors are logged, never propagated.
- **Async**: tokio multi-thread; blocking work behind `spawn_blocking`; concurrency bounded by semaphores, never unbounded spawns.
- **sqlx**: runtime `query`/`query_as` only — **no macros**, no compile-time `DATABASE_URL`, no `.sqlx` cache. `FromRow` derive only. Migrations embedded via `sqlx::migrate!` and auto-applied by every subcommand (`open_store`, `main.rs:134-136`); idempotent via `_sqlx_migrations`.
- **Dependencies**: exact `=` pins, hand-duplicated across both crate manifests (no `[workspace.dependencies]`) — a version bump must edit **both** manifests + `cargo update -p <crate>`. Feature `agent` (default) gates `rig-core`.
- **Config** (`crates/librarian/src/config.rs`): `ConfigFile` is `#[serde(deny_unknown_fields)]` — adding a key means adding the struct field or every run with the old file fails. Search order `--config` → `$BOOKSHELF_CONFIG` → `./librarian.toml`; env `BOOKSHELF_DATABASE_URL` always overrides the file; `~` expanded; derived paths (`mirror_dir`, `meta_dir`, `events_path`) are never file-settable.
- **Naming**: event kinds are dot-namespaced verbs (`book.discovered`, `file.transferred`, `feed.checked`, …); provider module `gutenberg_org` vs source key `project-gutenberg`; DB rows all carry `source`.
- **Commits**: `<binary>: <imperative lowercase summary>` + structured body (see `git log`).

## Important Files

- `crates/librarian/src/main.rs` — entry point, subcommand dispatch, daemon loop (`run_daemon`, `execute_job`, `scheduler_tick`)
- `crates/librarian/src/config.rs` — every tunable + defaults
- `crates/librarian/src/provider.rs` — Provider trait + registry (`resolve`, `ensure_known_key`)
- `crates/librarian/src/queue.rs` — enqueue/coalesce/pick/requeue semantics
- `crates/librarian/src/gutenberg_org/{mod,mirror,feed,rdf,taxonomy}.rs` — provider internals; taxonomy seed is a hardcoded snapshot (RDF leaves are source of truth; unknown leaves → `Unassigned`)
- `crates/bookshelf-core/src/domain.rs` — domain types + ports + the trait-discipline rationale
- `librarian-smoke.toml` — the only safe config for disk-touching runs (`library_dir=./library-smoke`, `feed_check_days=0`, `backfill_on_start=false`)

## Runtime/Tooling Preferences

- Rust stable, edition 2024 (≥1.85); Docker image builds on `rust:1.97-slim-bookworm`.
- `rsync ≥ 3.x` on PATH (daemon only; `sync`/`repair` clients build no providers).
- Postgres 16 (compose provides it; host port published on `127.0.0.1:5432` only).
- Container runs UID/GID 1000:1000; library is a bind mount so host and container binaries share state.
- cargo is the only build tool; no Makefile/justfile/task runner.

## Testing & QA

- 22 tests: per-crate integration tests in `crates/*/tests/` (RDF parse vs verbatim `fixtures/pg1342.rdf`, feed, itemize, triage ladder, taxonomy, event log, queue semantics) + 2 inline unit tests (`triage_agent.rs:211-240`, reply parsing).
- Fixtures live in `crates/librarian/tests/fixtures/` and load via `CARGO_MANIFEST_DIR`-relative paths (not `include_str!`); parser tests elsewhere use inline `const LINES` arrays.
- **DB-gated tests self-skip** by early `return` when `BOOKSHELF_DATABASE_URL` is unset — they are not `#[ignore]`d and pass vacuously without the var. They also **mutate the DB they point at** (probe rows `source='store-test'`, job-kind deletes): point them at a scratch DB.
- Smoke tier: run the real binary with `librarian-smoke.toml`, verify by reading `library-smoke/events.jsonl` — no assertions involved.
- Known untested surfaces — add fixture coverage when touching: CLI/daemon lifecycle (SIGTERM, `--wait`, scheduler), `config.rs` loading, `PoliteClient`, `RsyncRunner` process handling, most of the store beyond the roundtrip, ingest→DB path.
