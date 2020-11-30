update public.statement_data set transaction_description = REPLACE(transaction_description, '  ', ' ')

delete from public.statement_data;
select * from public.statement_data order by transaction_order;

select count(*), statement_date, sum(transaction_amount)
from public.statement_data
group by statement_date;

select date_part('year', statement_date) as year,date_part('month', statement_date) as month, count(*), sum(transaction_amount)
from public.statement_data
group by year, month
order by year,month;

select
date_part('year', statement_date) as year,
count(*) as "total_statements",
sum(transaction_amount),
round(avg(transaction_amount),4) as "average_amount" from public.statement_data
group by year
order by year;

select * form public.statement_date
where statement_date <= '2017-05-30'

select
date_part('year', statement_date) as year,
date_part('month', statement_date) as month,
count(*) as "total_statements",
sum(transaction_amount),
round(avg(transaction_amount),4) as "average_amount"
from public.statement_data
group by year, month
order by year, month;

select
count(*) as qty,
UPPER(transaction_description) tr_desc,
sum(transaction_amount) as total_amount,
round(avg(transaction_amount),4) as avg_amount,
min(transaction_amount),
max(transaction_amount),
min(statement_date) as "first",
max(statement_date) as "last",
age(max(statement_date),min(statement_date)) as "age",
date_part('year', age(max(statement_date),min(statement_date))) * 12 + date_part('month', age(max(statement_date),min(statement_date))) as "months"
from public.statement_data
group by tr_desc
order by tr_desc
