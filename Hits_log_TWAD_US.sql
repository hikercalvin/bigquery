WITH
  Hits_log AS(
  SELECT
    date,
    TIMESTAMP_SECONDS(ga.visitStartTime) AS timestamp,
    ga.fullVisitorId AS Users,
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
    totals.hits,
    hit.type,
    hit.hitnumber,
    hit.time,
    hit.page.pagepath,
    device.browser,
    device.LANGUAGE,
    geoNetwork.continent,
    geoNetwork.subContinent,
    geoNetwork.country AS geo_country,
    geoNetwork.region,
    geoNetwork.metro,
    geoNetwork.city,
    ga.totals.timeOnSite AS time_on_site,
    ga.totals.timeOnScreen AS time_on_screen,
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
      c.index= 50) AS Registered_User,
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
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 41) AS Network_Type,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 42) AS Bluetooth,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 43) AS GPS,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 44 ) AS Push_Opt_in,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 45 ) AS Ad_Blocking,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 46 ) AS Registered_Session,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 62 ) AS OMO_Product,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 19 ) AS Content_Category,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 1 ) AS Content_ID,
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
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 2 week ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    and regexp_contains(ga.geoNetwork.country, 'United States|Canada')

UNION ALL
 
  SELECT
    date,
    TIMESTAMP_SECONDS(ga.visitStartTime) AS timestamp,
    ga.fullVisitorId AS Users,
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
    totals.hits,
    hit.type,
    hit.hitnumber,
    hit.time,
    hit.page.pagepath,
    device.browser,
    device.LANGUAGE,
    geoNetwork.continent,
    geoNetwork.subContinent,
    geoNetwork.country AS geo_country,
    geoNetwork.region,
    geoNetwork.metro,
    geoNetwork.city,
    ga.totals.timeOnSite AS time_on_site,
    ga.totals.timeOnScreen AS time_on_screen,
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
      c.index= 50) AS Registered_User,
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
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 41) AS Network_Type,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 42) AS Bluetooth,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 43) AS GPS,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 44 ) AS Push_Opt_in,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 45 ) AS Ad_Blocking,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 46 ) AS Registered_Session,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 62 ) AS OMO_Product,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 19 ) AS Content_Category,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 1 ) AS Content_ID,
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
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 2 week ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    and regexp_contains(ga.geoNetwork.country, 'United States|Canada')
  
  UNION ALL
  
  SELECT
    date,
    TIMESTAMP_SECONDS(ga.visitStartTime) AS timestamp,
    ga.fullVisitorId AS Users,
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
    totals.hits,
    hit.type,
    hit.hitnumber,
    hit.time,
    hit.page.pagepath,
    device.browser,
    device.LANGUAGE,
    geoNetwork.continent,
    geoNetwork.subContinent,
    geoNetwork.country AS geo_country,
    geoNetwork.region,
    geoNetwork.metro,
    geoNetwork.city,
    ga.totals.timeOnSite AS time_on_site,
    ga.totals.timeOnScreen AS time_on_screen,
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
      c.index= 50) AS Registered_User,
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
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 41) AS Network_Type,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 42) AS Bluetooth,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 43) AS GPS,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 44 ) AS Push_Opt_in,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 45 ) AS Ad_Blocking,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 46 ) AS Registered_Session,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 62 ) AS OMO_Product,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 19 ) AS Content_Category,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 1 ) AS Content_ID,
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
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 2 week ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    and regexp_contains(ga.geoNetwork.country, 'United States|Canada'))

  
,View_log AS(
  SELECT
    date,
    hit.page.pagepath,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 19 ) AS Content_Category,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 29 ) AS Platform,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 1 ) AS Content_ID,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 10 ) AS Content_Title,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 26 ) AS Content_Publish_Date
  FROM
    `ga360-203507.178178607.ga_sessions_*` ga,
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 2 week ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)
,combine AS (
  SELECT
    date,
    pagepath,
    Content_ID,
    Content_Title,
    Content_Category,
    Platform,
    Content_Publish_Date,
    ROW_NUMBER() OVER(PARTITION BY Content_ID ORDER BY Content_Publish_Date DESC)AS ROW
  FROM
    View_log
  where Platform="WEB"
  )
 
,Content_ID_TW as(
SELECT
  Content_ID,
  Content_Title,
  Content_Category,
  Content_Publish_Date,
  ROW
FROM
  combine
where ROW=1)

,Hits_log_tw AS(
  SELECT
    date,
    timestamp,
    Client_ID as Users,
    Sessions,
    OMO_ACCID as OMO_Members,
    bounces,
    visitNumber AS Sessions_count,
    hits AS hits_count,
    hitnumber,
    type AS hit_type,
    time AS hit_time_log,
    pageviews AS total_Pageviews,
    screenviews AS total_Screenviews,
    Content_View,
    Video_View,
    channelGrouping,
    source,
    medium,
    referralPath,
    pagepath,
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
    Platform,
    Lon_Lat,
    Country,
    State,
    Language_Setting,
    OMO_Product,
    ID_date.Content_ID,
    ID_date.Content_Title,
    ID_date.Content_Category,
    ID_date.Content_Publish_Date
  FROM
    Hits_log SL
  LEFT JOIN
    Content_ID_TW ID_date
  ON
    SL.Content_ID=ID_date.Content_ID
  ORDER BY
    timestamp)

SELECT
  *
FROM
  Hits_log_tw
where regexp_contains(hit_type,'PAGE|APPVIEW')
ORDER BY
  timestamp


--where regexp_contains(OMO_Members, '^5')

--WHERE
    --_TABLE_SUFFIX =FORMAT_DATE('%Y%m%d', @run_date))


    --_TABLE_SUFFIX BETWEEN '20190620'
    --AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))


    --_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 week ))
    --AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

    --_TABLE_SUFFIX BETWEEN '20190701' AND '20190731'