select 
    s.*
from 
    {{ source('fetch_challenge', 'receipts') }} as r
join 
    {{ ref('stg_fetch__receipt_items_with_brands') }} as s 
on r."_id" = s.receipt_id
where 
    user_id in (select "_id" from {{ ref('stg_fetch__users_last_6_months') }})
