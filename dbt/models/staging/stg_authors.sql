
WITH authors AS (
    SELECT
    *
    FROM
    scilit_db.authors
)
SELECT
    author_id AS AuthorID,
    given_name AS FirstName,
	family_name AS LastName,
	affiliation AS Affiliation,
    IFNULL(affiliation, 'Anonymous') AS AffiliationCleansed
FROM
    authors