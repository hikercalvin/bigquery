WITH
  combine_Subscribers AS(
  SELECT
    ZS.Product__Name,
    ZS.Product_Rate_Plan__Name,
    ZS.Rate_Plan_Charge__Original_ID,
    ZS.Rate_Plan_Charge__Created_Date,
    ZS.Amendment__Type,
    ZS.Account__User_ID,
    ZS.Account__Account_Number,
    ZU.Account__Account_Number AS Unsubscribers
  FROM
    `nd-data-poc.zuora.ZuoraSubscribe` ZS
  LEFT JOIN
    `nd-data-poc.zuora.ZuoraUnsubscribe` ZU
  ON
    ZS.Account__Account_Number=ZU.Account__Account_Number)
  
,action_log AS(
  SELECT
    geoNetwork.country as geo_country,
    geoNetwork.region as geo_region,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 104) AS OMO_ACCID,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 51) AS User_Gender,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 52) AS User_DOB
   
  FROM
    `ga360-203507.195776449.ga_sessions_*` ga,
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX BETWEEN '20190618'
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

)
,row_set AS (
    SELECT
      OMO_ACCID,
      User_Gender,
      User_DOB,
      geo_country,
      geo_region,
      ROW_NUMBER() OVER(PARTITION BY OMO_ACCID ORDER BY User_Gender, User_DOB, geo_country, geo_region DESC)AS ROW
    FROM
      action_log)
,combine as(
    SELECT
      OMO_ACCID,
      User_Gender,
      User_DOB,
      geo_country,
      geo_region,
      ROW
    from
      row_set
    where ROW=1)

SELECT
    CS.Product__Name,
    CS.Product_Rate_Plan__Name,
    CS.Rate_Plan_Charge__Original_ID,
    CS.Rate_Plan_Charge__Created_Date,
    CS.Amendment__Type,
    CS.Account__User_ID,
    CS.Account__Account_Number,
    Unsubscribers,
    OMO_ACCID,
    User_Gender,
    User_DOB,
    geo_country,
    geo_region,
    ROW
  FROM
    combine_Subscribers CS
  INNER JOIN
    combine C
  ON
    CS.Account__Account_Number=C.OMO_ACCID
  order by Account__Account_Number 