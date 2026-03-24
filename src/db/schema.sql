CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE countries (
  iso3         CHAR(3)   PRIMARY KEY,
  iso2         CHAR(2)   UNIQUE,
  name         TEXT      NOT NULL,
  imf_code     INTEGER,
  wb_code      CHAR(3),
  bis_code     CHAR(2),
  un_m49       CHAR(3),
  is_active    BOOLEAN   NOT NULL DEFAULT TRUE
);

CREATE TABLE indicators (
  canonical_id TEXT  PRIMARY KEY,
  name         TEXT  NOT NULL,
  category     TEXT  NOT NULL,
  unit         TEXT  NOT NULL,
  frequency    TEXT  NOT NULL CHECK (frequency IN ('annual', 'quarterly', 'monthly')),
  description  TEXT
);

CREATE TABLE indicator_source_map (
  canonical_id    TEXT     NOT NULL REFERENCES indicators(canonical_id),
  source          TEXT     NOT NULL CHECK (source IN ('IMF', 'WORLD_BANK', 'FRED', 'BIS', 'OECD')),
  source_code     TEXT     NOT NULL,
  source_database TEXT,
  priority        INTEGER  NOT NULL DEFAULT 1,
  PRIMARY KEY (canonical_id, source)
);

CREATE TABLE macro_data (
  id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  country_iso3  CHAR(3)      NOT NULL REFERENCES countries(iso3),
  canonical_id  TEXT         NOT NULL REFERENCES indicators(canonical_id),
  period        TEXT         NOT NULL,
  frequency     TEXT         NOT NULL CHECK (frequency IN ('annual', 'quarterly', 'monthly')),
  value         NUMERIC,
  unit          TEXT         NOT NULL,
  source        TEXT         NOT NULL,
  source_code   TEXT         NOT NULL,
  last_updated  TIMESTAMPTZ  NOT NULL,
  ingested_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  UNIQUE (country_iso3, canonical_id, period, frequency, source)
);

CREATE INDEX idx_macro_data_lookup ON macro_data (country_iso3, canonical_id, frequency);
CREATE INDEX idx_macro_data_period ON macro_data (period);

CREATE TABLE ingestion_log (
  id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  source          TEXT         NOT NULL,
  started_at      TIMESTAMPTZ  NOT NULL,
  completed_at    TIMESTAMPTZ,
  status          TEXT         NOT NULL CHECK (status IN ('running', 'success', 'failed')),
  records_written INTEGER,
  error_message   TEXT
);

CREATE TABLE source_metadata (
  source          TEXT  PRIMARY KEY,
  display_name    TEXT  NOT NULL,
  base_url        TEXT  NOT NULL,
  last_ingested   TIMESTAMPTZ,
  next_scheduled  TIMESTAMPTZ,
  cron_schedule   TEXT  NOT NULL
);

INSERT INTO source_metadata (source, display_name, base_url, cron_schedule) VALUES
  ('IMF',        'International Monetary Fund',       'https://www.imf.org/external/datamapper/api', '0 2 * * 1'),
  ('WORLD_BANK', 'World Bank',                        'https://api.worldbank.org/v2',                '0 3 * * 1'),
  ('FRED',       'St. Louis Fed (FRED)',               'https://api.stlouisfed.org/fred',             '0 4 * * *'),
  ('BIS',        'Bank for International Settlements', 'https://stats.bis.org/api/v1',                '0 5 * * 1'),
  ('OECD',       'OECD',                              'https://sdmx.oecd.org/public/rest',            '0 6 * * 1');