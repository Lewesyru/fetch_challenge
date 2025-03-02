select "name" as brand, brand_code, count("name") as count
from {{ ref('stg_fetch__receipt_items_with_brands') }} as riwb
where
    riwb.receipt_id in (select _id from {{ ref('stg_fetch__receipts_last_month') }})
    and (deleted != true or deleted is null)
group by "name", brand_code
order by count desc
limit 5
