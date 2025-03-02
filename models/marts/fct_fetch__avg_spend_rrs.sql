select
	rewards_receipt_status,
	round(avg(total_spent)::numeric, 2) as average_spent
from
	{{ source('fetch_challenge', 'receipts') }} as r
where
    rewards_receipt_status in ('REJECTED', 'FINISHED')
group by
	rewards_receipt_status