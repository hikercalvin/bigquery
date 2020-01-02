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
    _TABLE_SUFFIX BETWEEN '20190903'
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))

,left_join_zuora AS(
  SELECT
    *
  FROM
    Sessions_log AS SL
  LEFT JOIN
    `nd-data-poc.zuora.ZuoraSubscribe` ZS
  ON
    SL.OMO_ACCID=ZS.Account__Account_Number
  ORDER BY
    date DESC )
 
SELECT
   date
  ,Sessions as Session_ID 
  ,OMO_ACCID
  ,Account__Account_Number AS Subscribers
  ,Account__User_ID AS Subscriber_email
  ,Rate_Plan_Charge__Created_Date AS Subscribe_time
  ,Product__Name
  ,Product_Rate_Plan__Name
  ,Rate_Plan_Charge__Original_ID
  ,Amendment__Type
  ,sum(Content_View) as Total_Content_View
  ,sum(Video_View) as Total_Video_View
FROM
  left_join_zuora
group BY
  date,
  Sessions,
  OMO_ACCID,
  Account__Account_Number,
  Account__User_ID,
  Rate_Plan_Charge__Created_Date,
  Product__Name,
  Product_Rate_Plan__Name,
  Rate_Plan_Charge__Original_ID,
  Amendment__Type
order by Total_Content_View desc