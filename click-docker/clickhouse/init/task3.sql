CREATE TABLE IF NOT EXISTS task123.people
(
    craft String,
    name String,
    _inserted_at DateTime
)
ENGINE = MergeTree
ORDER BY (craft, name, _inserted_at);


CREATE MATERIALIZED VIEW IF NOT EXISTS task123.mv_raw_logs_to_people
TO task123.people
AS
SELECT
    JSONExtractString(person_raw, 'craft') AS craft,
    JSONExtractString(person_raw, 'name') AS name,
    created_at AS _inserted_at
FROM task123.raw_logs
ARRAY JOIN JSONExtractArrayRaw(raw_json, 'people') AS person_raw;