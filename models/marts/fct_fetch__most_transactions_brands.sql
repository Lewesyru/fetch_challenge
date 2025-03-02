select
	"name" as brand,
	receipt_id,
	count("name") as transaction_count
from
	{{ ref('stg_fetch__receipt_items_users_last_6_months') }}
where
	"name" is not null 
group by
	"name", receipt_id 
order by
	transaction_count desc
limit 5