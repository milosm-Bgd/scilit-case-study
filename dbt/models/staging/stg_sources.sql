
WITH sources AS (
    SELECT
    *
    FROM
    scilit_db.sources
)
SELECT
    source_id AS SourceID,
    source_name AS SourceName
FROM
    sources