select ri.*,
    b."name"
from 
    {{ source('fetch_challenge', 'receipt_items') }} as ri
left join
    {{ source('fetch_challenge', 'brands') }} as b 
on ri.brand_code = b.brand_code
    
