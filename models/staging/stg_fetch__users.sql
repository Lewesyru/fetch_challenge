select
    distinct on (_id)
    *
from
    {{ source('fetch_challenge', 'users') }}