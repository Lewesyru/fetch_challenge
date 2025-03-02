select
	rewards_receipt_status,
	sum(purchased_item_count) as items_purchased
from
	{{ source('fetch_challenge', 'receipts') }} as r
where
    rewards_receipt_status in ('REJECTED', 'FINISHED')
group by
	rewards_receipt_status 