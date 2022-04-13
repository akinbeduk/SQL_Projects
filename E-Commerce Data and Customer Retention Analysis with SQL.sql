--E-Commerce Project 

--1. Join all the tables and create a new table called combined_table. (market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)

SELECT distinct *
INTO   combined_table
FROM
(SELECT A.[Ord_id], A.[Prod_id], A.[Ship_id], A.[Cust_id], A.[Sales], A.[Discount],
		A.[Order_Quantity], A.[Product_Base_Margin], B.[Customer_Name], B.[Province],
		B.[Region], B.[Customer_Segment], C.[Order_Date], C.[Order_Priority], D.[Product_Category],
		D.[Product_Sub_Category], E.[Order_ID], E.[Ship_Mode], E.[Ship_Date]
FROM	   [dbo].[market_fact] A
LEFT JOIN  [dbo].[cust_dimen] B on A.Cust_id = B.Cust_id
LEFT JOIN  [dbo].[orders_dimen] C on A.Ord_id = C.Ord_id
LEFT JOIN  [dbo].[prod_dimen] D on A.Prod_id = D.Prod_id
LEFT JOIN  [dbo].[shipping_dimen] E on A.Ship_id = E.Ship_id) as newtable 


--2. Find the top 3 customers who have the maximum count of orders.

select distinct top 3  [Customer_Name], count(distinct Ord_id) Count_of_Orders
from combined_table
group by [Customer_Name]
order by count(distinct Ord_id) desc

--3.Create a new column at combined_table as DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date.
--Use "ALTER TABLE", "UPDATE" etc.

alter table combined_table
add DaysTakenForDelivery int

update combined_table
set DaysTakenForDelivery = datediff(day,Order_Date,Ship_Date)

--4. Find the customer whose order took the maximum time to get delivered.
--Use "MAX" or "TOP"

select top 1  [Customer_Name], DaysTakenForDelivery
from combined_table
order by DaysTakenForDelivery desc

--5. Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011
--You can use date functions and subqueries

SELECT datename(month, order_date) Month, Count(Distinct cust_id) Monthly_Number_of_Customer
FROM	Combined_table A
WHERE EXISTS
			(
			SELECT Cust_id
			FROM combined_table B
			WHERE Year (Order_Date) = 2011
			AND	Month (Order_Date) = 1
			AND A.Cust_id = B.Cust_id
			)
AND	Year(Order_Date) = 2011
GROUP BY datename(month, order_date)
ORDER BY Monthly_Number_of_Customer DESC

--6. write a query to return for each user acording to the time elapsed between the first purchasing and the third purchasing, 
--in ascending order by Customer ID
--Use "MIN" with Window Functions

WITH T1 AS
(
SELECT
		Cust_id
	  , [Ship_id]
	  , Ship_Date
	  , LEAD(Ship_Date,2)  over (partition by Cust_id order by Ship_Date) next_purchasing
	  , ROW_NUMBER () OVER (PARTITION BY Cust_id ORDER BY Ship_Date) row_num
	  , DATEDIFF(DAY, Ship_Date, (LEAD(Ship_Date,2)  over (partition by Cust_id order by Ship_Date))) day_elapsed
from combined_table
)
SELECT  Cust_id, day_elapsed
		FROM T1
		WHERE row_num = 1 and day_elapsed is not null
		
--7. Write a query that returns customers who purchased both product 11 and product 14, 
--as well as the ratio of these products to the total number of products purchased by all customers.
--Use CASE Expression, CTE, CAST and/or Aggregate Functions

WITH T1 AS
(
select Cust_id, Customer_Name, Order_Quantity, Ord_id, Prod_id
from combined_table
where Prod_id='Prod_11'
 ), T2 AS
(
select Cust_id, Customer_Name,  Order_Quantity, Ord_id, Prod_id
from combined_table
where Prod_id='Prod_14'
), T3 AS
(
SELECT
		A.Cust_id
	  , A.Customer_Name
	  , A.Order_Quantity as p11
	  , B.Order_Quantity as p14
	  , A.Prod_id as id11
	  , B.Prod_id as id14
	  , sum(A.Order_Quantity + B.Order_Quantity ) over () total
FROM T1 A
INNER JOIN T2 B on A.Cust_id=B.Cust_id
), Tson as
(
select distinct total
from T3
union
select sum(Order_Quantity)
from combined_table
)
select top 1 lag(total) over(order by total) / total *100 sonuc
from Tson
order by sonuc desc

--CUSTOMER SEGMENTATION

--1. Create a view that keeps visit logs of customers on a monthly basis. (For each log, three field is kept: Cust_id, Year, Month)
--Use such date functions. Don't forget to call up columns you might need later.

create view visit_logs as
select distinct cust_id, YEAR(Order_Date) Year_of_visit, MONTH(Order_Date) Month_of_visit
from combined_table

--2.Create a “view” that keeps the number of monthly visits by users. (Show separately all months from the beginning  business)
--Don't forget to call up columns you might need later.

create view monthly_visit as
SELECT YEAR(Order_Date) years, MONTH(Order_Date) months,  COUNT(Ord_id)visit_quants
FROM combined_table
GROUP BY YEAR(Order_Date), MONTH(Order_Date)
order by YEAR(Order_Date), MONTH(Order_Date)

--3. For each visit of customers, create the next month of the visit as a separate column.
--You can order the months using "DENSE_RANK" function.
--then create a new column for each month showing the next month using the order you have made above. (use "LEAD" function.)
--Don't forget to call up columns you might need later.

with t1 as
(
SELECT distinct Cust_id, ord_id, YEAR(Order_Date) years, MONTH(Order_Date) months,
	DENSE_RANK() OVER(PARTITION BY Cust_id ORDER BY YEAR(Order_Date), MONTH(Order_Date) )  DENSE_RANK_1, Order_Date
	FROM combined_table
	--order by cust_id, YEAR(Order_Date), MONTH(Order_Date)
), t2 as
(
select Cust_id, ord_id,years, months, order_date
		,LEAD(years)  OVER(PARTITION BY Cust_id ORDER BY YEAR(Order_Date))next_visit_year
		,LEAD(months)  OVER(PARTITION BY Cust_id ORDER BY YEAR(Order_Date))next_visit_month
		-- İki sipariş arasında tarih farkının bir ay olacak şekilde ve yıl geçişleri dikkate alınarak hesaplanmıştır.
		,CASE					
		WHEN (years=LEAD(years)  OVER(PARTITION BY Cust_id ORDER BY YEAR(Order_Date))) 
		and (LEAD(months)  OVER(PARTITION BY Cust_id ORDER BY YEAR(Order_Date)))-months =1
		THEN 1 
		WHEN (LEAD(years) OVER(PARTITION BY Cust_id ORDER BY YEAR(Order_Date))) - years =1 
		and months - (LEAD(months)  OVER(PARTITION BY Cust_id ORDER BY YEAR(Order_Date))) =11
		THEN 1 END SONUC
		from t1
)
		select *
		from t2
		where sonuc is not null
		order by Cust_id, SONUC, YEAR(Order_Date), MONTH(Order_Date)
			   
--4. Calculate monthly time gap between two consecutive visits by each customer.
--Don't forget to call up columns you might need later.

WITH T1 AS
(
SELECT
		Cust_id
	  , [Ord_id]
	  , Order_Date
	  , LEAD(Order_Date)  over (partition by Cust_id order by Order_Date) next_visit
	  , ROW_NUMBER () OVER (PARTITION BY Cust_id ORDER BY Order_Date) row_num
	  , DATEDIFF(MONTH, Order_Date, (LEAD(Order_Date)  over (partition by Cust_id order by Order_Date))) month_elapsed
from combined_table
)
SELECT  Cust_id,Ord_id,Order_Date,next_visit, month_elapsed
		FROM T1
		WHERE  month_elapsed is not null and month_elapsed!=0
		order by Cust_id, Order_Date

--5.Categorise customers using average time gaps. Choose the most fitted labeling model for you.
--For example: 
--Labeled as “churn” if the customer hasn't made another purchase for the months since they made their first purchase.
--Labeled as “regular” if the customer has made a purchase every month.
--Etc.


with t1 as
(
SELECT  distinct
			Cust_id
		  , YEAR(Order_Date) years
		  , MONTH(Order_Date) months
		
FROM combined_table
)
select distinct
		  Cust_id
	    , count(years) OVER(PARTITION BY Cust_id ORDER BY Cust_id)  countyears
		, case
		  when count(years) OVER(PARTITION BY Cust_id ORDER BY Cust_id) = 1 then 'Churn'        -----4 yıl boyunca sadece 1 yıl ziyarette bulunmuş
		  when count(years) OVER(PARTITION BY Cust_id ORDER BY Cust_id) = 2  then 'irregular'	-----4 yıl boyunca sadece 2 yıl ziyarette bulunmuş
		  when count(years) OVER(PARTITION BY Cust_id ORDER BY Cust_id) = 3  then 'irregular'   -----4 yıl boyunca 3 yıl ziyarette bulunmuş
		  when count(years) OVER(PARTITION BY Cust_id ORDER BY Cust_id) = 4  then 'regular'     -----4 yıl boyunca her yıl ziyarette bulunmuş ziyaret sayısı ise yıl içinde farklılılar gösteriyor
		  else 'Unknown' end labels
from t1
order by Cust_id

--MONTH-WISE RETENTION RATE


--Find month-by-month customer retention rate  since the start of the business.


--1. Find the number of customers retained month-wise. (You can use time gaps)
--Use Time Gaps

CREATE VIEW CUST_MONTH AS
WITH T1 AS 
(
SELECT DISTINCT Cust_id, year (order_date) ord_year, month(Order_Date) ord_month,
				dense_rank () OVER (ORDER BY year (order_date) , month(Order_Date)) data_month
FROM combined_table
)
SELECT DISTINCT cust_id, data_month, LAG(data_month) OVER (PARTITION BY cust_id ORDER BY data_month) prev_data_month
FROM t1

CREATE VIEW TIME_GAP AS
SELECT *,  data_month-prev_data_month time_gaps
FROM CUST_MONTH

SELECT data_month, count (DISTINCT cust_id) total_cust
FROM TIME_GAP
GROUP BY data_month


--2. Calculate the month-wise retention rate.

--Basic formula: o	Month-Wise Retention Rate = 1.0 * Number of Customers Retained in The Current Month / Total Number of Customers in the Current Month

--It is easier to divide the operations into parts rather than in a single ad-hoc query. It is recommended to use View. 
--You can also use CTE or Subquery if you want.

--You should pay attention to the join type and join columns between your views or tables.

WITH T1 AS 
(
SELECT data_month, count (DISTINCT cust_id) total_cust
FROM TIME_GAP
GROUP BY data_month
), T2 AS
(
SELECT data_month, count (DISTINCT cust_id) retained_cust
FROM TIME_GAP
WHERE time_gaps = 1
GROUP BY data_month
) 
SELECT t1.data_month, CAST(1.0*retained_cust/total_cust AS NUMERIC(3,2)) Retention_Rate, FORMAT(((1.0*retained_cust)/total_cust), 'P', 'en-us')
FROM T1, T2
WHERE T1.data_month = T2.data_month

---///////////////////////////////////
