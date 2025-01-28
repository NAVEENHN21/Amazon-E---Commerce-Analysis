use ecommerce;
-- ------------------------------------------------------------------------------------------------------------------------------
-- Qno 14
-- Identify the top 5 most valuable customers using a composite score that       combines three key metrics: (SQL)
-- Total Revenue (50% weight): The total amount of money spent by the customer.
-- Order Frequency (30% weight): The number of orders placed by the customer, indicating their loyalty and engagement.
-- Average Order Value (20% weight): The average value of each order placed by the customer, reflecting the typical transaction size.

with customermetrics as (
    select
        o.customerid,
        sum(o.saleprice * o.orderquantity + o.shippingfee) as totalrevenue,
        count(o.orderid) as orderfrequency,
        avg(o.saleprice * o.orderquantity + o.shippingfee) as averageordervalue
    from orders o
    group by o.customerid
),
maxvalues as (
    select 
        max(totalrevenue) as maxtotalrevenue,
        max(orderfrequency) as maxorderfrequency,
        max(averageordervalue) as maxaverageordervalue
    from customermetrics
),
weightedscores as (
    select
        cm.customerid,
        cm.totalrevenue,
        cm.orderfrequency,
        cm.averageordervalue,
        (0.50 * cm.totalrevenue / mv.maxtotalrevenue) +
        (0.30 * cm.orderfrequency / mv.maxorderfrequency) +
        (0.20 * cm.averageordervalue / mv.maxaverageordervalue) as compositescore
    from customermetrics cm
    cross join maxvalues mv
)
select 
    c.customerid,
    ws.totalrevenue,
    ws.orderfrequency,
    ws.averageordervalue,
    ws.compositescore
from weightedscores ws
join customers c on ws.customerid = c.customerid
order by ws.compositescore desc
limit 5;
-- --------------------------------------------------------------------------------------------------------
-- Qno 15
-- Calculate the month-over-month growth rate in total revenue across the entire dataset. (SQL)

with monthlyrevenue as (
    select
        date_format(str_to_date(orderdate, '%d/%m/%Y'), '%Y-%m') as month,
        sum(saleprice * orderquantity + shippingfee) as totalrevenue
    from orders
    group by month
),
revenueswithgrowth as (
    select
        mr.month,
        mr.totalrevenue,
        lag(mr.totalrevenue) over (order by mr.month) as previousmonthrevenue
    from monthlyrevenue mr
)
select
    month,
    totalrevenue,
    previousmonthrevenue,
    case
        when previousmonthrevenue is null then null
        when previousmonthrevenue = 0 then null
        else 
            ((totalrevenue - previousmonthrevenue) / previousmonthrevenue) * 100
    end as momgrowthrate
from revenueswithgrowth
order by month;

-- ------------------------------------------------------------------------------------------------------------
-- Qno 16
-- Calculate the rolling 3-month average revenue for each product category. (SQL)

with monthlyrevenue as (
    select
        date_format(str_to_date(orderdate, '%d/%m/%Y'), '%Y-%m') as month,
        productcategory,
        sum(saleprice * orderquantity + shippingfee) as totalrevenue
    from orders
    group by month, productcategory
),
rollingavgrevenue as (
    select
        month,
        productcategory,
        totalrevenue,
        avg(totalrevenue) over (partition by productcategory order by month rows between 2 preceding and current row) as rolling3monthavgrevenue
    from monthlyrevenue
)
select 
    month,
    productcategory,
    totalrevenue,
    rolling3monthavgrevenue
from rollingavgrevenue
order by productcategory, month;
-- ----------------------------------------------------------------------------------------------------------
-- Qno 17
-- Update the orders table to apply a 15% discount on the `Sale Price` for orders placed by customers who have made at least 10 orders. (SQL)

set sql_safe_updates = 0;
update orders o
join (
    select customerid
    from orders
    group by customerid
    having count(orderid) >= 10
) as eligible_customers
on o.customerid = eligible_customers.customerid
set o.saleprice = o.saleprice * 0.85;

-- ----------------------------------------------------------------------------------------------------------------------------
-- Qno 18 Calculate the average number of days between consecutive orders for customers who have placed at least five orders. (SQL)

with eligiblecustomers as (
    select 
        customerid
    from 
        orders
    group by 
        customerid
    having 
        count(orderid) >= 5
),
orderintervals as (
    select 
        o.customerid,
        o.orderdate,
        datediff(o.orderdate, lag(o.orderdate) over (
            partition by o.customerid order by o.orderdate
        )) as daysbetweenorders
    from 
        orders o
    where 
        o.customerid in (select customerid from eligiblecustomers)
),
averagedayspercustomer as (
    select 
        customerid,
        avg(daysbetweenorders) as avgdaysbetweenorders
    from 
        orderintervals
    where 
        daysbetweenorders is not null
    group by 
        customerid
)
select 
    avg(avgdaysbetweenorders) as overallavgdaysbetweenorders
from 
    averagedayspercustomer;



-- --------------------------------------------------------------------------------------------------------------
-- Qno 19
-- Identify customers who have generated revenue that is more than 30% higher than the average revenue per customer. (SQL)

with customerrevenue as (
    select 
        customerid, 
        sum(saleprice * orderquantity) as totalrevenue
    from orders
    group by customerid
),
averagerevenue as (
    select 
        avg(totalrevenue) as avgrevenue
    from customerrevenue
)
select 
    cr.customerid,
    cr.totalrevenue,
    ar.avgrevenue,
    (cr.totalrevenue / ar.avgrevenue - 1) * 100 as revenuepercentageaboveaverage
from 
    customerrevenue cr
join 
    averagerevenue ar
where 
    cr.totalrevenue > (ar.avgrevenue * 1.30)
order by 
    cr.totalrevenue desc;
    
    
-- -------------------------------------------------------------------------------------------------------
-- Qno 20 Determine the top 3 product categories that have shown the highest increase in sales over the past year compared to the previous year. (SQL)
with yearlycategorysales as (
    select 
        date_format(str_to_date(orderdate, '%d/%m/%Y'), '%Y') as salesyear,
        productcategory,
        sum(saleprice) as totalsales
    from 
        orders
    group by 
        salesyear, productcategory
),
categorysalesgrowth as (
    select 
        ycs.productcategory,
        ycs.salesyear,
        ycs.totalsales,
        lag(ycs.totalsales) over (partition by ycs.productcategory order by ycs.salesyear) as previousyearsales,
        (ycs.totalsales - lag(ycs.totalsales) over (partition by ycs.productcategory order by ycs.salesyear)) as salesincrease
    from 
        yearlycategorysales ycs
),
rankedcategories as (
    select 
        csg.productcategory,
        csg.salesyear,
        csg.totalsales,
        csg.previousyearsales,
        csg.salesincrease,
        dense_rank() over (order by csg.salesincrease desc) as `rank`
    from 
        categorysalesgrowth csg
    where 
        csg.salesyear = (select max(salesyear) from orders)
)
select 
    productcategory,
    salesyear,
    round(totalsales, 2) as totalsales,
    round(previousyearsales, 2) as previousyearsales,
    round(salesincrease, 2) as salesincrease
from 
    rankedcategories
where 
    `rank` <= 3
order by 
    salesincrease desc;



