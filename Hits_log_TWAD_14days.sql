WITH
  Hits_log AS(
  SELECT
    date,
    TIMESTAMP_SECONDS(ga.visitStartTime) AS timestamp,
    ga.clientId AS Client_ID,
    CONCAT(fullVisitorId,CAST(visitId AS string)) AS Sessions,
    totals.bounces,
    visitNumber,
    totals.pageviews,
    totals.screenviews,
    ga.channelGrouping,
    ga.trafficSource.source,
    ga.trafficSource.medium,
    ga.trafficSource.referralPath,
    device.browser,
    device.LANGUAGE,
    geoNetwork.continent,
    geoNetwork.subContinent,
    geoNetwork.country AS geo_country,
    geoNetwork.region,
    geoNetwork.metro,
    geoNetwork.city,
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
      c.index= 52) AS User_DOB,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 29 ) AS Platform,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 60 ) AS Lon_Lat,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 61 ) AS Country,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 62 ) AS State,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 40) AS Language_Setting,
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
    `ga360-203507.178178607.ga_sessions_*` ga,
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

UNION ALL

    SELECT
    date,
    TIMESTAMP_SECONDS(ga.visitStartTime) AS timestamp,
    ga.clientId AS Client_ID,
    CONCAT(fullVisitorId,CAST(visitId AS string)) AS Sessions,
    totals.bounces,
    visitNumber,
    totals.pageviews,
    totals.screenviews,
    ga.channelGrouping,
    ga.trafficSource.source,
    ga.trafficSource.medium,
    ga.trafficSource.referralPath,
    device.browser,
    device.LANGUAGE,
    geoNetwork.continent,
    geoNetwork.subContinent,
    geoNetwork.country AS geo_country,
    geoNetwork.region,
    geoNetwork.metro,
    geoNetwork.city,
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
      c.index= 52) AS User_DOB,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 29 ) AS Platform,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 60 ) AS Lon_Lat,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 61 ) AS Country,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 62 ) AS State,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 40) AS Language_Setting,
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
    `ga360-203507.75307673.ga_sessions_*` ga,
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
 
UNION ALL

    SELECT
    date,
    TIMESTAMP_SECONDS(ga.visitStartTime) AS timestamp,
    ga.clientId AS Client_ID,
    CONCAT(fullVisitorId,CAST(visitId AS string)) AS Sessions,
    totals.bounces,
    visitNumber,
    totals.pageviews,
    totals.screenviews,
    ga.channelGrouping,
    ga.trafficSource.source,
    ga.trafficSource.medium,
    ga.trafficSource.referralPath,
    device.browser,
    device.LANGUAGE,
    geoNetwork.continent,
    geoNetwork.subContinent,
    geoNetwork.country AS geo_country,
    geoNetwork.region,
    geoNetwork.metro,
    geoNetwork.city,
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
      c.index= 52) AS User_DOB,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 29 ) AS Platform,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 60 ) AS Lon_Lat,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 61 ) AS Country,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 62 ) AS State,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 40) AS Language_Setting,
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
    `ga360-203507.75701445.ga_sessions_*` ga,
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))    
    
    )

,combine AS (
  SELECT
    date,
    timestamp,
    Client_ID as Users,
    Sessions,
    OMO_ACCID as OMO_Members,
    bounces,
    visitNumber AS Sessions_count,
    pageviews AS total_Pageviews,
    screenviews AS total_Screenviews,
    channelGrouping,
    source,
    medium,
    referralPath,
    browser,
    LANGUAGE,
    continent as geo_continent,
    subContinent as geo_subContinent,
    geo_country,
    region as geo_region,
    metro,
    city,
    User_Gender,
    User_DOB,
    Lon_Lat,
    Country,
    State,
    Language_Setting,
    Platform,
    sum(Content_View) as Total_Content_View,
    sum(Video_View) as Total_Video_View,
    ROW_NUMBER() OVER(PARTITION BY Sessions ORDER BY Platform DESC)AS ROW
  FROM
    Hits_log
  group by
    date,
    timestamp,
    Client_ID,
    Sessions,
    OMO_Members,
    bounces,
    Sessions_count,
    pageviews,
    screenviews,
    channelGrouping,
    source,
    medium,
    referralPath,
    browser,
    LANGUAGE,
    continent,
    subContinent,
    geo_country,
    region,
    metro,
    city,
    User_Gender,
    User_DOB,
    Lon_Lat,
    Country,
    State,
    Language_Setting,
    Platform)
    
SELECT
   *
FROM
  combine
where ROW=1
ORDER BY
  Sessions

--regexp_contains(hit_type, 'PAGE|APPVIEW')
--regexp_contains(ga.geoNetwork.country, 'United States|Canada')
--regexp_contains(OMO_Members, '^5')

--WHERE
    --_TABLE_SUFFIX =FORMAT_DATE('%Y%m%d', @run_date))


    --_TABLE_SUFFIX BETWEEN '20190620'
    --AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))


    --_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 week ))
    --AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

    --_TABLE_SUFFIX BETWEEN '20190701' AND '20190731'