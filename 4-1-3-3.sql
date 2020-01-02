WITH
  daily_purchase AS (
  SELECT
    date,
    SUBSTR(date, 1, 4) AS year,
    SUBSTR(date, 5, 2) AS month,
    SUBSTR(date, 7, 2) AS day,
    SUM(Total_Content_View) AS Total_Content_View,
    SUM(Total_Video_View) AS Total_Video_View
  FROM
    `ga360-203507.sample_data_GA.4_1_subscribers`
  GROUP BY
    date )
SELECT
  date
  ,concat(year, '-', month) as year_month
  ,Total_Content_View
  ,Total_Video_View
  ,sum(Total_Content_View) over(partition by year,month order by date rows unbounded preceding) as agg_Content_View
  ,sum(Total_Video_View) over(partition by year,month order by date rows unbounded preceding) as agg_Video_View
FROM
  daily_purchase
ORDER BY
  date