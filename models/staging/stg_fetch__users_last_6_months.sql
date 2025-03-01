select
    distinct on (_id)
    *
from
    {{ source('fetch_challenge', 'users') }}
where 
    created_date > (
        select max(created_date) from {{ source('fetch_challenge', 'users') }}
        ) - '6 months'::interval