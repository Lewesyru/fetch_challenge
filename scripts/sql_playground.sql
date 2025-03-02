-- receipts from last month & previous month
select * from yiru.receipts as r 
where date_scanned > (select max(date_scanned) from yiru.receipts) - '2 months'::interval
and date_scanned < (select max(date_scanned) from yiru.receipts) - '1 months'::interval


select  from yiru.stg_fetch__receipt_items_with_brands as s
where s.receipt_id in (select _id from yiru.stg_fetch__receipts_last_month as sfrlm)
and (deleted != true or deleted is null)

with receipts_last_month as(
	select * from receipts
	where date_scanned >= (select max(date_scanned) from receipts) - '1 month'::interval
),
receipt_items_with_brands as (
	select ri.*, b."name" from receipt_items as ri
	left join brands as b 
	on ri.brand_code = b.brand_code
)
select "name" as brand, brand_code, count("name") as count
from receipt_items_with_brands as riwb
where riwb.receipt_id in (select _id from receipts_last_month)
and (deleted != true or deleted is null)
group by "name", brand_code
order by count desc
limit 5
---------------------------------------------

select barcode, count(barcode) from yiru.brands as b 
group by barcode 
having count(barcode) > 1

-- data quality issue found!
select barcode, brand_code, "name"  from yiru.brands as b 
where barcode in 
('511111605058','511111204923','511111704140',
'511111504788','511111504139','511111305125','511111004790')
order by barcode

---------------------------------------------
select * from yiru.users as u 
order by _id

select _id, count(*) from users
group by _id
order by count desc
limit 5

-- users created within past 6 months
SELECT DISTINCT ON (_id) * FROM yiru.users
WHERE created_date > (select max(created_date) from yiru.users) - '6 months'::interval

select * from yiru.stg_fetch__users_last_6_months

---------------------------------------------
select
	rewards_receipt_status,
	round(avg(total_spent)::numeric,
	2) as average_spent
from
	receipts as r
group by
	rewards_receipt_status

select
	rewards_receipt_status,
	sum(purchased_item_count)
from
	receipts as r
group by
	rewards_receipt_status 


---------------------------------------------
select * from yiru.receipts as r 
join yiru.stg_fetch__receipt_items_with_brands as s 
on r."_id" = s.receipt_id 
where user_id in (select "_id" from yiru.stg_fetch__users_last_6_months)
	
	
select
	"name" as brand,
	count("name") as count
from
	yiru.stg_fetch__receipt_items_last_6_months
where
	"name" is not null 
group by
	"name"
order by
	count desc
limit 10


select
	"name" as brand,
	count("name") as transaction_count
from
	yiru.stg_fetch__receipt_items_last_6_months
where
	"name" is not null 
group by
	"name", receipt_id 
order by
	transaction_count desc
limit 5