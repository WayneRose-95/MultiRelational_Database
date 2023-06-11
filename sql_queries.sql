-- Question 1: How many stores does the business have and in which countries? 

-- Question 1 Solution? 

SELECT country_code as Country, COUNT(country_code) as total_no_of_stores FROM dim_store_details
WHERE country_code in ( 'GB', 'DE', 'US')
GROUP BY country_code;

-- Question 2: Which locations currently have the most stores? 

-- Question 2 Solution? 

SELECT city as locality, COUNT(city) as total_no_of_stores FROM dim_store_details
GROUP BY locality
ORDER BY total_no_of_stores DESC;

-- Question 3: Which month produce the average highest cost of sales typically? 

-- Question 3 Solution? 

SELECT  

ROUND(SUM(x.product_price * O.product_quantity)::numeric,2) AS total_sales, 
D.month

FROM DIM_PRODUCT_DETAILS x

JOIN ORDERS_TABLE O ON 

x.PRODUCT_KEY = O.product_key
JOIN DIM_DATE_TIMES D ON 
O.date_key = D.time_key
GROUP BY D.month
ORDER BY total_sales DESC;

-- Question 4: How many sales are coming from online? 

/* SELECT store_type FROM DIM_STORE_DETAILS;

SELECT 

DS.store_type,
SUM(O.product_quantity) as product_quantity_count


FROM 
DIM_STORE_DETAILS DS
JOIN ORDERS_TABLE O ON 
DS.STORE_KEY = O.STORE_KEY 

JOIN DIM_PRODUCT_DETAILS DP ON 
O.PRODUCT_KEY = DP.PRODUCT_KEY

GROUP BY store_type; */

-- Question 4 Solution? 

SELECT
    CASE
        WHEN DS.store_type IN ('local', 'Mail Kiosk', 'Outlet', 'Super Store') THEN 'Offline'
        ELSE 'Web Portal'
    END AS store_type,
    SUM(O.product_quantity) as product_quantity_count,
	COUNT(O.CARD_NUMBER) as number_of_sales
FROM
    DIM_STORE_DETAILS DS
    JOIN ORDERS_TABLE O ON DS.STORE_KEY = O.STORE_KEY
    JOIN DIM_PRODUCT_DETAILS DP ON O.PRODUCT_KEY = DP.PRODUCT_KEY
GROUP BY
    CASE
        WHEN DS.store_type IN ('local', 'Mail Kiosk', 'Outlet', 'Super Store') THEN 'Offline'
        ELSE 'Web Portal'
    END;



-- Question 5: What percentage of sales come through each type of store? 

-- Question 5 Solution? 

-- Select the distinct store_type 
SELECT DISTINCT store_type FROM dim_store_details;

SELECT 

DS.store_type,
ROUND(SUM(DP.product_price * O.product_quantity)::numeric,2) AS total_sales,
ROUND((SUM(DP.product_price * O.product_quantity) / SUM(SUM(DP.product_price * O.product_quantity)) OVER ())::numeric * 100, 2) AS percentage_total

FROM DIM_STORE_DETAILS DS

JOIN ORDERS_TABLE O ON 
DS.STORE_KEY = O.STORE_KEY 

JOIN DIM_PRODUCT_DETAILS DP ON 
O.PRODUCT_KEY = DP.PRODUCT_KEY
GROUP BY DS.store_type
ORDER BY total_sales DESC;



-- Question 6: Which month in each year produced the highest cost of sales? 

-- Question 6 Solution? 

SELECT 
ROUND(SUM(DS.product_price * O.product_quantity)::numeric,2) AS total_sales,
DT.YEAR,
DT.MONTH
FROM 

DIM_PRODUCT_DETAILS DS 

JOIN ORDERS_TABLE O ON 

DS.PRODUCT_KEY = O.PRODUCT_KEY 

JOIN DIM_DATE_TIMES DT ON 
O.DATE_KEY = DT.TIME_KEY

GROUP BY DT.YEAR, DT.MONTH;

-- Question 7: What is our staff headcount? 

-- Question 7 Solution? 

SELECT 

SUM(DS.NUMBER_OF_STAFF) as number_of_staff,
DS.COUNTRY_CODE

FROM 

DIM_STORE_DETAILS DS 

GROUP BY DS.COUNTRY_CODE
ORDER BY number_of_staff DESC;


-- Question 8: Which German store type is selling the most? 

-- Question 8: Solution? 

SELECT
ROUND(SUM(DP.product_price * O.product_quantity)::numeric,2) AS total_sales,
DS.STORE_TYPE,
DS.COUNTRY_CODE 

FROM DIM_STORE_DETAILS DS 

JOIN ORDERS_TABLE O ON 

DS.STORE_KEY = O.STORE_KEY

JOIN DIM_PRODUCT_DETAILS DP 

ON O.PRODUCT_KEY = DP.PRODUCT_KEY

WHERE COUNTRY_CODE LIKE 'DE'

GROUP BY DS.STORE_TYPE, DS.COUNTRY_CODE
ORDER BY total_sales ASC;


-- Question 9: How quickly is the company making sales? 

-- Determine the average time taken between each sale grouped by year. 

