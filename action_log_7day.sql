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
    hit.eventInfo.eventCategory,
    hit.eventInfo.eventAction,
    hit.eventInfo.eventLabel,
    hit.eventInfo.eventValue,
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
    `ga360-203507.178178607.ga_sessions_*` ga,
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

union all

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
    hit.eventInfo.eventCategory,
    hit.eventInfo.eventAction,
    hit.eventInfo.eventLabel,
    hit.eventInfo.eventValue,
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
    `ga360-203507.75307673.ga_sessions_*` ga,
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

union all

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
    hit.eventInfo.eventCategory,
    hit.eventInfo.eventAction,
    hit.eventInfo.eventLabel,
    hit.eventInfo.eventValue,
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
    `ga360-203507.75701445.ga_sessions_*` ga,
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

)

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
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY ))
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
    eventCategory as Event_Category,
    eventAction as Event_Action,
    eventLabel as Event_Label,
    eventValue as Event_Value,
    pageviews AS total_Pageviews,
    screenviews AS total_Screenviews,
    timeOnSite as total_timeOnSite,
    Navigation_Channel,
    Navigation_Section,
    Page_Type,
    Content_News_Type,
    Content_Source,
    Registered_Session,
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
    Content_Block,
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

,combine_sub as(
    select 
      date,
      timestamp,
      Users,
      Sessions,
      OMO_Members,
      bounces,
      Sessions_count,
      hits_count,
      hitnumber,
      hit_type,
      hit_time_log,
      Event_Category,
      Event_Action,
      Event_Label,
      Event_Value,
      total_Pageviews,
      total_Screenviews,
      total_timeOnSite,
      Navigation_Channel,
      Navigation_Section,
      Page_Type,
      Content_News_Type,
      Content_Source,
      Registered_Session,
      Content_View,
      Video_View,
      channelGrouping,
      source,
      medium,
      referralPath,
      pagepath,
      browser,
      LANGUAGE,
      geo_continent,
      geo_subContinent,
      geo_country,
      geo_region,
      metro,
      city,
      User_Gender,
      User_DOB,
      Content_Block,
      Platform,
      Lon_Lat,
      Country,
      State,
      Language_Setting,
      OMO_Product,
      Content_ID,
      Content_Title,
      Content_Category,
      Content_Publish_Date,
      NUll as action,
    FROM Hits_log_tw
      
union all
   
   select  
      NULL as date,
      Rate_Plan_Charge__Created_Date as timestamp,
      NULL as Users,
      NULL as Sessions,
      Account__Account_Number as OMO_Members,
      NULL as bounces,
      NULL as Sessions_count,
      NULL as hits_count,
      NULL as hitnumber,
      NULL as hit_type,
      NULL as hit_time_log,
      NULL as Event_Category,
      NULL as Event_Action,
      NULL as Event_Label,
      NULL as Event_Value,
      NULL as total_Pageviews,
      NULL as total_Screenviews,
      NULL as total_timeOnSite,
      NULL as Navigation_Channel,
      NULL as Navigation_Section,
      NULL as Page_Type,
      NULL as Content_News_Type,
      NULL as Content_Source,
      NULL as Registered_Session,
      NULL as Content_View,
      NULL as Video_View,
      NULL as channelGrouping,
      NULL as source,
      NULL as medium,
      NULL as referralPath,
      NULL as pagepath,
      NULL as browser,
      NULL as LANGUAGE,
      NULL as geo_continent,
      NULL as geo_subContinent,
      NULL as geo_country,
      NULL as geo_region,
      NULL as metro,
      NULL as city,
      NULL as User_Gender,
      NULL as User_DOB,
      NULL as Content_Block,
      NULL as Platform,
      NULL as Lon_Lat,
      NULL as Country,
      NULL as State,
      NULL as Language_Setting,
      NULL as OMO_Product,
      NULL as Content_ID,
      NULL as Content_Title,
      NULL as Content_Category,
      NULL as Content_Publish_Date,
      "Subscribe" as action       
    FROM `nd-data-poc.zuora.ZuoraSubscribe` )

SELECT
   *
FROM
  combine_sub
ORDER BY
   timestamp