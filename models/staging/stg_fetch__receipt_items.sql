select
    *
from
    {{ source('fetch_challenge', 'receipt_items') }}