WITH
  Sessions_log AS(
  SELECT
    date,
    TIMESTAMP_SECONDS(ga.visitStartTime) AS timestamp,
    ga.fullVisitorId AS Users,
    ga.clientId AS Client_ID,
    CONCAT(fullVisitorId,CAST(visitId AS string)) AS Sessions,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 104) AS OMO_ACCID,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customMetrics) x
    WHERE
      x.index = 2 ) AS Content_View,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customMetrics) x
    WHERE
      x.index = 5 ) AS Video_View
  FROM
    `ga360-203507.195776449.ga_sessions_*` ga,
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX BETWEEN '20190618'
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) ),
  
  sum_log AS(
  SELECT
    date,
    Sessions AS Session_ID,
    OMO_ACCID,
    SUM(Content_View) AS Total_Content_View,
    SUM(Video_View) AS Total_Video_View
  FROM
    Sessions_log
  GROUP BY
    date,
    Sessions,
    OMO_ACCID
  ORDER BY
    Total_Content_View DESC),
  
  combine AS(
  SELECT
    date,
    Session_ID,
    OMO_ACCID,
    ROW_NUMBER() OVER(PARTITION BY OMO_ACCID ORDER BY OMO_ACCID DESC)AS ROW,
    Account__Account_Number,
    Total_Content_View,
    Total_Video_View
  FROM
    sum_log AS SL
  LEFT JOIN
    `nd-data-poc.zuora.ZuoraSubscribe` ZS
  ON
    SL.OMO_ACCID=ZS.Account__Account_Number
  WHERE
    REGEXP_CONTAINS(Account__Account_Number, '^5')
  ORDER BY
    OMO_ACCID DESC)

SELECT
  *
FROM
  combine
WHERE
  ROW=1