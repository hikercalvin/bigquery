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
  day,
  SUM(CASE month
      WHEN "07" THEN Total_Content_View
  END
    ) AS amount_Content_View_07,
  SUM(CASE month
      WHEN "08" THEN Total_Content_View
  END
    ) AS amount_Content_View_08,
  SUM(CASE month
      WHEN "07" THEN Total_Video_View
  END
    ) AS amount_Video_View_07,
  SUM(CASE month
      WHEN "08" THEN Total_Video_View
  END
    ) AS amount_Video_View_08,
  100.0*SUM(CASE month
      WHEN "08" THEN Total_Content_View
  END
    ) / SUM(CASE month
      WHEN "07" THEN Total_Content_View
  END
    ) AS Rate_Content_View,
  100.0*SUM(CASE month
      WHEN "08" THEN Total_Video_View
  END
    ) / SUM(CASE month
      WHEN "07" THEN Total_Video_View
  END
    ) AS Rate_Video_View
FROM
  daily_purchase
GROUP BY
  day
ORDER BY
  day