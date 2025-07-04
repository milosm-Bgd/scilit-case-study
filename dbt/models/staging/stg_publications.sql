{{ config(
    materialized='table'
) }}

with raw as (
  select * from {{ source('raw','publications') }}
),
-- 1) Filter out rows missing critical fields
filtered as (
  select *
  from raw
  where publication_id is not null
    and title <> ''
    and publisher <> ''
    and type <> ''
    and source_id  is not null
    and issued_date_id  is not null
),
-- 2) Clean HTML entities, trim whitespace, normalize case
cleaned as (
  select
    publication_id,
    lower(trim(doi)) as doi,
    replace(trim(title), '&+;+$', '') as title,
    trim(publisher) as publisher,
    source_id,
    issued_date_id,
    type
  from filtered
),
-- 3) Deduplicate by DOI – keep latest issued_date_id
deduplicated as (
  select *
  from (select *,
                row_number() over( partition by doi order by issued_date_id desc) as row_num
        from cleaned) t
  where row_num = 1
)
select
  publication_id,
  doi,
  title,
  publisher,
  type,
  source_id,
  issued_date_id
from deduplicated