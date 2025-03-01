select distinct on (_id) * from yiru.brands as b 


select barcode, count(barcode)  from yiru.brands as b 
group by barcode 
having count(barcode) > 1

-- data quality issue found!
select * from yiru.brands as b 
where barcode in ('511111605058',
'511111204923',
'511111704140',
'511111504788',
'511111504139',
'511111305125',
'511111004790')


-- users created within past 6 months
SELECT DISTINCT ON (_id) * FROM yiru.users
WHERE created_date > (select max(created_date) from yiru.users) - '6 months'::interval

