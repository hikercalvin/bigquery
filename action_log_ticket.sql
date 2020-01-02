with
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
    totals.timeOnSite,
    ga.channelGrouping,
    ga.trafficSource.source,
    ga.trafficSource.medium,
    ga.trafficSource.referralPath,
    totals.hits,
    hit.type,
    hit.hitnumber,
    hit.time,
    hit.page.pagepath,
    hit.page.hostname,
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
      UNNEST(hit.customDimensions) c
    WHERE
      c.index= 16) AS Navigation_Channel,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index= 17) AS Navigation_Section,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index= 21) AS Page_Type,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index= 22) AS Content_News_Type,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index= 23) AS Content_Source,
    (
    SELECT
      c.value
    FROM
      UNNEST(ga.customDimensions) c
    WHERE
      c.index= 46) AS Registered_Session,
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
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index= 36) AS Content_Block,  
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
      c.index = 60 ) AS Lon_Lat,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 61 ) AS Country,
    (
    SELECT
      c.value
    FROM
      UNNEST(hit.customDimensions) c
    WHERE
      c.index = 62 ) AS State,
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
    `ga360-203507.180497877.ga_sessions_*` ga,
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    and regexp_contains(hit.page.hostname, 'tw.ticket.appledaily.com')
)

select
  *
from
  Hits_log
