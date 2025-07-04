{{ config(
    materialized='view'
) }}

select
    id, 
    publication_id AS PublicationID,
    author_id AS AuthorID,
    sequence AS Sequence

from {{ source('raw', 'publication_authors') }}
