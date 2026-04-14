SELECT
    Month,
    AVG(Weekly_Sales) AS avg_weekly_sales
FROM fact_sales
GROUP BY Month
