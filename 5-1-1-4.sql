WITH
  Session_log AS(
  SELECT
    date,
    visitId,
    ga.fullVisitorId AS Visitor_ID,
    ga.totals.timeOnSite AS time_on_site,
    visitStartTime AS visit_time,
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
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 month ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) ),
  action_log_with_status AS(
  SELECT
    visitId,
    OMO_ACCID,
    Registered_Session,
    CASE
      WHEN coalesce(MAX(OMO_ACCID)OVER(PARTITION BY visitId ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),  '')<>'' THEN 'member'
    ELSE
    'none'
  END
    AS member_status,
    date
  FROM
    session_log )
SELECT
  *
FROM
  action_log_with_status