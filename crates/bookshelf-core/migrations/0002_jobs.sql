CREATE TABLE jobs (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source TEXT NOT NULL,                     -- provider key
  kind TEXT NOT NULL,                       -- 'full_cycle' | 'feed_cycle' | 'repair'
  payload JSONB NOT NULL DEFAULT '{}',      -- CycleOpts: {"only":[1342],"limit":50,"no_ingest":false}
  origin TEXT NOT NULL,                     -- 'schedule' | 'cli'
  priority INTEGER NOT NULL,                -- 0 = schedule (always wins), 10 = cli
  status TEXT NOT NULL DEFAULT 'queued',    -- queued|running|done|failed
  attempts INTEGER NOT NULL DEFAULT 0,
  run_id BIGINT,                            -- set to sync_runs.id once executed
  error TEXT,
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);
CREATE INDEX jobs_pickup ON jobs (status, priority, enqueued_at);
