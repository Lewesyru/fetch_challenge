select *
from {{ source('fetch_challenge', 'receipts') }}
where
    date_scanned >= (
        select max(date_scanned) 
        from {{ source('fetch_challenge', 'receipts') }}
        ) - '1 month'::interval
