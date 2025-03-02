select
	"name" as brand,
	round(sum(final_price)::numeric, 2) as spent_sum
from
	{{ ref('stg_fetch__receipt_items_users_last_6_months') }}
where
	"name" is not null
group by
	"name"
order by
	spent_sum desc
limit 5