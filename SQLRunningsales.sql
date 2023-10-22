/*1- write a sql to find top 3 products in each category 
by highest rolling 3 months total sales for Jan 2020. */

with xxx as (select category,product_id,datepart(year,order_date) as yo,datepart(month,order_date) as mo, sum(sales) as sales
from Orders 
group by category,product_id,datepart(year,order_date),datepart(month,order_date))
,yyyy as (
select *,sum(sales) over(partition by category,product_id order by yo,mo rows between 2 preceding and current row ) as roll3_sales
from xxx)
select * from (
select *,rank() over(partition by category order by roll3_sales desc) as rn from yyyy 
where yo=2020 and mo=1) A
where rn<=3


--2- write a query to find products for which month over month sales has never declined.

with cte as (
select product_name, DATEPART(month, order_date) as mnt,DATEPART(YEAR, order_date) as yr , SUM(sales) as sales 
from Orders
group by product_name, datepart(month, order_date), datepart(year, order_date) ), cte1 as (
select *, lag(sales, 1,0) over( PARTITION by product_name  order by yr, mnt ) as r_sales
from cte ) select distinct product_name  from cte1 
where product_name not in (select product_name from cte1 where sales<r_sales)

/*3- write a query to find month wise sales for each category for months
where sales is more than the combined sales of previous 2 months for that category. */

with cte as (
select category, DATEPART(year, order_date) as yr, DATEPART(month, order_date) as mnt , 
SUM(sales) as sales
from Orders
group by category, DATEPART(year, order_date), DATEPART(month, order_date) ), cte2 as (
select *, SUM(sales) over(partition by category order by yr, mnt rows between 2 preceding and 1 preceding ) as prev_2sales
from cte)
select * from cte2
where sales>prev_2sales
