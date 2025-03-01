select
    *
from
    {{ source('fetch_challenge', 'receipts') }}