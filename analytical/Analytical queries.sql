-- Anallytics queries 

-- Counting the number of Publications per Source

SELECT
  s.source_name,
  COUNT(*) AS publication_count
FROM scilit_db_scilit_stg.stg_publications p
JOIN scilit_db.sources s
  ON p.source_id = s.source_id
GROUP BY s.source_name
ORDER BY publication_count DESC;


-- Average Authors per Publication by Year

SELECT
  d.year,
  ROUND(AVG(author_count), 2) AS avg_authors
FROM (
  SELECT
    p.publication_id,
    dt.year,
    COUNT(pa.author_id) AS author_count
  FROM scilit_db_scilit_stg.stg_publications p
  JOIN scilit_db_scilit_stg.stg_dates dt
    ON p.issued_date_id = dt.date_id
  JOIN scilit_stg.stg_publication_authors pa
    ON p.publication_id = pa.publication_id
  GROUP BY p.publication_id, dt.year
) t
GROUP BY t.year
ORDER BY t.year;


-- Monthly Publication Trend

SELECT
  CONCAT(dt.year,'-',LPAD(dt.month,2,'0')) AS year_month,
  COUNT(*)                           AS pubs_in_month
FROM scilit_core.core_publications p
JOIN scilit_stg.stg_dates dt
  ON p.issued_date_id = dt.date_id
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month;

