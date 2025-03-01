select
    *
from
    {{ source('fetch_challenge', 'brands') }}