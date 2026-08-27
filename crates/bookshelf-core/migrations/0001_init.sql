CREATE TABLE books (
  source TEXT NOT NULL,                     -- source key, e.g. 'project-gutenberg'
  id BIGINT NOT NULL,                       -- external ebook id
  type TEXT NOT NULL DEFAULT 'Text',
  title TEXT NOT NULL,
  language TEXT NOT NULL DEFAULT 'en',
  issued DATE,                              -- release date from RDF dcterms:issued
  publisher TEXT,
  rights TEXT,
  description TEXT,                         -- marc520 summary
  reading_ease TEXT,
  downloads INTEGER,
  authors JSONB NOT NULL DEFAULT '[]',      -- [{name, birth, death, wikipedia}]
  subjects JSONB NOT NULL DEFAULT '[]',     -- [{scheme:"LCSH"|"LCC"|"Other", value}]
  bookshelves JSONB NOT NULL DEFAULT '[]',  -- raw shelf names, "Category: " prefix intact
  status TEXT NOT NULL DEFAULT 'discovered', -- discovered|enriched|synced|failed_permanent
  attempts INTEGER NOT NULL DEFAULT 0,
  retry_at TIMESTAMPTZ,                     -- repair backoff ladder target
  last_error TEXT,
  first_seen TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (source, id)
);
CREATE TABLE book_files (
  source TEXT NOT NULL,
  book_id BIGINT NOT NULL,
  format TEXT NOT NULL,                     -- 'txt'|'epub.images'|'html.zip'|'cover'
  url TEXT,                                 -- verbatim from RDF hasFormat (HTTP repair only)
  bytes_expected BIGINT,                    -- dcterms:extent
  remote_modified TEXT,                     -- dcterms:modified
  path TEXT,                                -- relative to library_dir, e.g. 'mirror/1342/pg1342-images.epub'
  status TEXT NOT NULL DEFAULT 'pending',   -- pending|done|skipped|failed
  attempts INTEGER NOT NULL DEFAULT 0,
  retry_at TIMESTAMPTZ,
  last_error TEXT,
  PRIMARY KEY (source, book_id, format),
  FOREIGN KEY (source, book_id) REFERENCES books(source, id)
);
CREATE TABLE categories (
  source TEXT NOT NULL,
  name TEXT NOT NULL,                       -- leaf name, e.g. 'Romance'
  parent TEXT,                              -- top group from seed; NULL → reported as 'Unassigned'
  bookshelf_id INTEGER,                     -- from /ebooks/bookshelf/{id} (seed entries only)
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (source, name)
);
CREATE TABLE book_categories (
  source TEXT NOT NULL,
  book_id BIGINT NOT NULL,
  category TEXT NOT NULL,
  PRIMARY KEY (source, book_id, category),
  FOREIGN KEY (source, book_id) REFERENCES books(source, id),
  FOREIGN KEY (source, category) REFERENCES categories(source, name)
);
CREATE TABLE sync_runs (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source TEXT NOT NULL,
  cycle TEXT NOT NULL DEFAULT 'full',       -- 'full' (weekly rsync delta) | 'feed' (daily today.rss)
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  rsync_exit INTEGER, transferred_files INTEGER, transferred_bytes BIGINT,
  new_books INTEGER, enriched INTEGER,
  files_failed INTEGER, files_skipped INTEGER,
  aborted_reason TEXT
);
CREATE TABLE meta (
  source TEXT NOT NULL,
  key TEXT NOT NULL,                        -- 'daemon_anchor', 'next_full_sync', 'daemon_heartbeat', 'last_feed_pub_date', 'last_rsync_host'
  value TEXT NOT NULL,
  PRIMARY KEY (source, key)
);
