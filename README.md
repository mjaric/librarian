# bookshelf

Provider-based archive synchronizer. The backend binary `librarian` keeps a
local mirror of the [Project Gutenberg](https://www.gutenberg.org) archive —
politely, via their sanctioned rsync service — and records book state in
PostgreSQL plus an append-only JSONL event log. First provider:
`gutenberg_org` (source key `project-gutenberg`).

## How it works

- **Weekly full rsync** mtime-delta pull of the `gutenberg-epub` module
  (generated collection: txt, EPUB-with-images, HTML zip, cover, per-book
  RDF metadata) — the mirror carries the metadata, so the site is never
  crawled. No HTML page is ever fetched.
- **Daily feed check** of the official `today.rss` (~2.7 KB) — new and
  updated books are re-pulled with a targeted rsync.
- **RDF ingest** from the local mirror fills `books`, `book_files`,
  `categories`, `book_categories`; categories come bottom-up from
  `pgterms:bookshelf` values, parents from a hardcoded seed snapshot.
  Every cycle also reconciles the mirror against the DB before ingesting
  (`reconcile=N` in the log), so files delivered by an interrupted run
  are picked up on the next run instead of being lost to rsync's
  mtime-delta.
- **HTTP repair** (gap-fill only) through a rate-limited client
  (1 request / `request_interval_ms`, ≤ `max_parallel_downloads` parallel),
  driven by deterministic triage rules; an optional LLM agent
  (`triage = "agent"`) handles ambiguous cases and always falls back to
  rules, so the keyless default is fully functional.
- **Job queue in Postgres**: `librarian sync` / `repair` are clients — they
  enqueue + `NOTIFY`. Only `librarian daemon` executes. Scheduled jobs
  (priority 0) always outrank CLI jobs (priority 10); pickup uses
  `FOR UPDATE SKIP LOCKED`.
- Single worker = serialized cycles — that is the politeness mechanism.
  rsync children run in their own process groups with `PR_SET_PDEATHSIG`,
  so no ghost process can outlive the daemon.

## Workspace layout

```
crates/bookshelf-core/   shared infra: domain + ports (EventSink, Triage),
                          Postgres store + migrations, JSONL event log,
                          PoliteClient, RsyncRunner, triage rules/agent
crates/librarian/        the binary: CLI + daemon, config, provider trait
                          + registry, job queue, gutenberg_org provider
                          (mirror / rdf / feed / taxonomy)
```

Hexagonal-lite: core owns source-agnostic adapters; everything
Gutenberg-specific lives behind the `gutenberg_org` module. A second source
later = a new module + one registry entry.

## Setup

Prerequisites: Rust (stable, edition 2024), `rsync` ≥ 3.x on PATH, Postgres
(or Docker for it).

```sh
# Postgres (skip if you already have one)
docker run --name bookshelf-pg \
  -e POSTGRES_USER=bookshelf -e POSTGRES_PASSWORD=bookshelf \
  -e POSTGRES_DB=bookshelf -p 5432:5432 -d postgres:16

export BOOKSHELF_DATABASE_URL=postgres://bookshelf:bookshelf@localhost:5432/bookshelf

cargo run -p librarian -- migrate   # applies sqlx migrations (idempotent)
```

`BOOKSHELF_DATABASE_URL` always overrides the config file — keep credentials
out of `librarian.toml`.

## Docker

```sh
docker compose up -d --build       # postgres:16 + librarian daemon
docker compose logs -f librarian   # wait for "daemon ready"
```

- The daemon runs as UID/GID **1000:1000**; the library is a plain bind
  mount `~/.bookshelf → /data` (mirror/, meta/, events.jsonl), so the files
  survive and stay owned by your host user — a non-docker `librarian` can
  pick them up later by setting `library_dir = "~/.bookshelf"` and pointing
  `BOOKSHELF_DATABASE_URL` at the same Postgres.
- Postgres state lives in the `bookshelf-pgdata` named volume; its port is
  published on **127.0.0.1 only** — that is the host CLI's door into the
  queue. Change the credentials for anything beyond a localhost setup.
- `docker compose stop librarian` sends SIGTERM → graceful shutdown
  (running job requeued, rsync children group-killed), exit 0.
- rsync source: the container config pins `rsync_host = "rsync.ibiblio.org"`
  (A/B winner, ~114× faster listing); the daemon automatically alternates
  with `gutenberg.pglaf.org` on retries, so the ladder never repeats a host.
- The first daemon start enqueues the full backfill (~230 GB, 1–3 days).
  Set `backfill_on_start = false` to control when that happens and start it
  yourself with `librarian sync`.

Sending commands from the host to the containerized daemon — same binary,
same database, the CLI enqueues + NOTIFYs, the daemon executes:

```sh
export BOOKSHELF_DATABASE_URL=postgres://bookshelf:bookshelf@localhost:5432/bookshelf
cargo run -p librarian -- sync --only 1342 --wait
```

Client subcommands (`sync`, `repair`) build no providers — they need neither
rsync nor the library dir on the host. For a standalone host binary:
`cargo install --path crates/librarian`. `status` reports DB state via the
same URL; its mirror-file walk is local — point `--config` at a config with
`library_dir = "~/.bookshelf"` to see the container's library.

## Configuration

Search order: `--config PATH` → `$BOOKSHELF_CONFIG` → `./librarian.toml`.
Every key is optional; defaults in parentheses.

```toml
database_url          = "postgres://bookshelf:bookshelf@localhost:5432/bookshelf"
library_dir           = "./library"          # mirror/, meta/, events.jsonl inside
rsync_host            = "gutenberg.pglaf.org" # fallback: rsync.ibiblio.org
rsync_module          = "gutenberg-epub"      # the generated collection
download_host         = "https://www.gutenberg.org" # feed + repair only
formats               = ["txt", "epub.images", "html.zip", "cover"]
max_parallel_downloads = 4                    # HTTP repair concurrency only
request_interval_ms   = 2000                  # spacing for ALL HTTP requests
timeout_secs          = 60                    # HTTP; rsync gets --timeout=600
max_total_attempts    = 12                    # repair ladder exhaustion
circuit_breaker       = 15                    # consecutive retriable → abort pass
full_sync_interval_days = 7                   # scheduler cadence
feed_check_days       = 1                     # 0 disables feed checks
backfill_on_start     = true                  # first daemon start pulls everything
contact_email         = ""                    # appended to the HTTP User-Agent
triage                = "rules"               # "rules" | "agent"

[agent]
provider = "zai"        # "zai" | "openai" | "ollama"
model    = "glm-5.3"

[observability]
otlp_endpoint = "http://host.docker.internal:4318/"  # opt-in; absent = fully off
```

Metrics are **off by default**: without `otlp_endpoint` there is no SDK, no
exporter, no network traffic. Set it to an OTLP/HTTP collector (**:4318**,
protobuf) and the daemon pushes every 30 s — `http://127.0.0.1:4318/` for a
host-run daemon, `http://host.docker.internal:4318/` for the containerized
one (compose already maps that name to the host via
`extra_hosts: ["host.docker.internal:host-gateway"]`). Exported metrics:

```text
librarian.books{status}                   books by status
librarian.files{status}                   book files by status
librarian.queue{status}                   queue depth (queued, running)
librarian.rsync_files                     files moved by the current rsync attempt
librarian.rsync_bytes                     bytes moved by the current rsync attempt
librarian.rsync_files_total               counter — files moved by rsync, monotonic since process start; use increase() for per-interval counts
librarian.rsync_bytes_total               counter — bytes moved by rsync, monotonic since process start; use increase() for per-interval bytes
librarian.rsync_last_item_age_seconds     silence since the last itemize line
librarian.heartbeat_age_seconds           daemon process liveness
librarian.mirror_books                    books with an RDF on the local mirror (recounted every 5 min)
librarian.ingest_gap                      mirror books not yet in the DB; heals to ~0 at each cycle end via reconcile
librarian.active_phase                    live cycle phase code — 0 idle, 1 listing, 2 transferring, 3 ingesting, 4 repairing
librarian.cycles{kind,outcome}            counter — completed cycles
librarian.cycle_duration_seconds{kind}    histogram — cycle wall-clock
```

Any OTLP/HTTP receiver works; the bundled stack lives in the separate
`~/prj/telemetry` compose project (collector scrape — not remote-write —
of the collector's `:8889` exporter → Prometheus `:9090` → Grafana `:3001`)
and receives all of the above, provisioned with a ready-made Librarian
dashboard. See the Monitoring section below.

Point `library_dir` at the big volume — a full mirror is ~230 GB (txt 15 GB +
epub.images 120–150 GB + html.zip 80–100 GB + rdf 1.5 GB + covers 3 GB).
First pull typically takes 1–3 days; weekly pulls are delta-only.

## Usage

```sh
librarian daemon                       # the backend: embedded scheduler + worker
librarian sync [--feed] [--limit N] [--only ID]... [--no-ingest] [--wait]
librarian repair [--only ID]... [--wait]
librarian status                       # daemon + active cycle + queue + provider report
librarian watch [--interval S]         # live dashboard (2 s default, Ctrl-C exits)
librarian runs [--limit N]             # recent sync_runs rows, newest first
librarian jobs [--limit N]             # recent queue jobs, newest first
librarian progress [--provider KEY]    # downloaded / total + % per source
librarian retry-failed
librarian migrate
```

`--wait` blocks the client until the job reaches a terminal state (usable
from cron against a running daemon). Data subcommands never execute cycles —
only the daemon does.

systemd (also in `librarian daemon --help`):

```ini
[Service]
ExecStart=/usr/local/bin/librarian daemon --config /etc/librarian.toml
Environment=BOOKSHELF_DATABASE_URL=postgres://...
Restart=on-failure
KillSignal=SIGTERM
TimeoutStopSec=30
```

SIGTERM/SIGINT stop job pickup, group-kill the active rsync, requeue the
interrupted job (≥3 interruptions → failed) and exit 0.

## Monitoring

`status`, `watch`, `runs` and `jobs` read the daemon's DB state directly —
cheap queries only, no provider work. Only `status` walks the local mirror,
once, because it is a one-off command; `watch` deliberately skips the walk
so it can refresh every couple of seconds (default 2, minimum 1; Ctrl-C
exits — read-only, nothing to clean up).

```sh
librarian status              # DAEMON + ACTIVE CYCLE + QUEUE + provider report
librarian watch               # the cheap views, refreshed live
librarian runs --limit 20     # sync_runs rows, newest first
librarian jobs  --limit 20    # jobs rows, newest first
```

`status` output (sketch):

```
DAEMON
  heartbeat: alive (3s ago)
  next full sync: 2026-09-05 11:33:31
  last feed check: 2026-08-29 03:00:02
  rsync host: gutenberg.pglaf.org

ACTIVE CYCLE
  full_cycle — job 42 run 7 — phase transferring, elapsed 5m 12s
  files: 1234, bytes: 9.42 MiB (31.0 KiB/s)
  host: gutenberg.pglaf.org
  last item: 5s ago
  ⚠ no items for 3m (trickle/stall?)

QUEUE
  queued: 0, running: 1

provider: project-gutenberg
  books discovered: 55000
  ...
```

- **heartbeat** — the `daemon_heartbeat` meta key is a *process-alive*
  signal: the worker loop rewrites it every 5 s while idle, and the
  scheduler tick keeps it warm every 30 s while a long cycle blocks the
  worker. Under 90 s old → `alive`; older → `STALE`, i.e. the daemon
  process is probably gone and queued jobs will not move.
- **active_run** — while a cycle runs, the daemon publishes a JSON snapshot
  under the `active_run` meta key every 5 s and clears it on every exit
  path (clean end, interrupt, and the startup crash-guard against ghost
  runs). Phases: `listing` (remote module walk) → `transferring` (rsync
  itemize lines) → `ingesting` (RDF) / `repairing` (HTTP gap-fill). The
  transfer rate shows only while `transferring`.
- **stall hints** — rsync can sit minutes without a new itemize line while
  trickling one huge file, and the first listing is the *remote's* walk,
  not ours; both look like a hang from the outside, so the CLI says so:
  `⚠ no items for Xm (trickle/stall?)` when `transferring` has seen no new
  item for over 120 s, and `⚠ listing still running (server-side walk)`
  when the listing phase passes 300 s.
- **self-healing** — each cycle reconciles the local mirror against the
  DB (`reconcile=N`) before ingesting: interrupted runs heal on the next
  cycle, and a first backfill shows a large N once. The repair pass
  stats the disk before HTTP: a file already present in the mirror is
  marked done without a download (`repair skipped download`).

`runs` and `jobs` render plain aligned tables — in-flight runs show
`· running` with a live duration, and job errors are flattened and
truncated to ~60 characters:

```
RUNS — project-gutenberg, last 20
id  cycle  started→finished                         duration  files  bytes  new  enriched  aborted
3  full   2026-08-29 11:46:36→2026-08-29 11:51:36  5m 0s         0    0 B    0         0  interrupted
1  full   2026-08-29 11:33:31→· running            32m 55s       ·      ·    ·         ·  ·
```

### Observability stack (metrics + dashboards)

The daemon's OTLP metrics land in a self-hosted observability stack living in
a **separate compose project** at `~/prj/telemetry` (up/down independently
from bookshelf's own `docker-compose.yml` — the librarian container config
`docker/librarian.toml` already points at it):

```sh
docker compose -f ~/prj/telemetry/compose.yml up -d      # start the stack
docker compose -f ~/prj/telemetry/compose.yml down       # stop it again
```

Local endpoints (all host-network):

| Surface                     | URL                            |
| --------------------------- | ------------------------------ |
| Grafana (Librarian dashboard, admin/admin) | http://127.0.0.1:3001 |
| Prometheus (query UI/API)   | http://127.0.0.1:9090          |
| Jaeger UI (traces)          | http://127.0.0.1:16686         |
| OTLP collector (HTTP)       | http://127.0.0.1:4318 (`/v1/metrics`, `/v1/traces`) |

Chain: the librarian pushes OTLP/HTTP to the collector on `:4318`; the
collector exposes them on its Prometheus exporter `:8889` (metric names are
sanitized: `librarian.books` → `librarian_books`, counters gain `_total`),
Prometheus scrapes `:8889` every 5 s, and Grafana renders the provisioned
**Librarian — Bookshelf Backfill** dashboard (tag `librarian`): rsync
progress + rate, the 120 s rsync-silence stall detector, the 90 s heartbeat
threshold, queue/books/files by status, and cycle rate/duration.

## State

- **Postgres**: `books`, `book_files`, `categories`, `book_categories`,
  `sync_runs`, `meta`, `jobs` — every row carries its `source`.
- **`{library_dir}/events.jsonl`**: append-only audit trail, one JSON object
  per line (`book.discovered`, `file.transferred`, `file.repaired`,
  `feed.checked`, …), tagged with `source`. Never rewritten or truncated.
- **`{library_dir}/meta/{id}.json`**: per-book sidecar (full record +
  categories + file paths), written on the transition to `synced`.

## Verification
```sh
cargo build && cargo test        # unit + fixture tests (no DB needed)
BOOKSHELF_DATABASE_URL=... cargo test   # + queue/store/monitoring DB tests
cargo run -p librarian -- status         # daemon + active cycle + queue + provider report
cargo run -p librarian -- runs           # recent sync_runs rows
cargo run -p librarian -- jobs           # recent queue rows
```

The RDF parser is fixture-tested against a verbatim `pg1342.rdf`; triage,
itemize parsing, event log, taxonomy and feed parsing all have fixture tests.

## Contributing

- `AGENTS.md` is binding for humans and agents alike.
- **Never add a dependency younger than 2 weeks.** All dependencies are
  exact-pinned (`=a.b.c`) and `Cargo.lock` is committed; upgrades are
  deliberate single-line edits followed by `cargo update -p <crate>`.
- Hexagonal, but not at the cost of maintainability — new provider? New
  module + one registry entry. New backend? Extract the trait from real
  usage first.
- **Keep this README (setup, configuration, usage, verification) accurate
  after every change** — a change that isn't documented here isn't done.
