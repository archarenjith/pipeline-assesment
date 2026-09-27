-- Database to hold pipeline control/metadata (separate from analytics data)
CREATE DATABASE IF NOT EXISTS meta;

-- Tracks the last successfully-extracted updated_at per source table.
-- ReplacingMergeTree keyed by table_name means the latest write for a
-- given table_name wins after merges (or when queried with FINAL).
CREATE TABLE IF NOT EXISTS meta.ingestion_watermarks (
    table_name String,
    last_watermark DateTime64(6),
    last_run_id String,
    last_success_at DateTime64(6) DEFAULT now()
) ENGINE = ReplacingMergeTree(last_success_at)
ORDER BY table_name;