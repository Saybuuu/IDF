CREATE DATABASE IF NOT EXISTS task123;

CREATE TABLE IF NOT EXISTS task123.raw_logs
(
    dedup_key String,
    raw_json String,
    created_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY dedup_key;