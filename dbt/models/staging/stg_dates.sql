
WITH dates AS (
    SELECT
    *
    FROM
    scilit_db.dates
)
SELECT
    date_id AS DateID,
    year AS Year,
	CASE WHEN month IS NULL THEN 1 ELSE month END AS Month ,
	CASE WHEN day IS NULL THEN 1 ELSE day END AS Day
FROM
    dates