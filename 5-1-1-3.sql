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
    `ga360-203507.178178607.ga_sessions_20190616`ga,
    UNNEST(hits) AS hit ),
  action_log_with_status AS(
  SELECT
    visitId,
    OMO_ACCID,
    Registered_Session,
    CASE
      WHEN coalesce(OMO_ACCID,  '')<>'' THEN 'login'
    ELSE
    'guest'
  END
    AS login_status
  FROM
    Session_log ),
  sub_category_amount AS(
  SELECT
    Registered_Session AS Registered_Session,
    login_status AS login_status,
    COUNT(DISTINCT visitId) AS action_visit,
    COUNT(visitId) AS action_count_visit
  FROM
    action_log_with_status
  GROUP BY
    Registered_Session,
    login_status ),
  category_amount AS(
  SELECT
    Registered_Session,
    'all' AS login_status,
    COUNT(DISTINCT visitId) AS action_visit,
    COUNT(visitId) AS action_count_visit
  FROM
    action_log_with_status
  GROUP BY
  Registered_Session ),
  
  total_amount AS(
SELECT
  'all' as Registered_Session,
  'all' as login_status,
  COUNT(DISTINCT visitId) AS action_visit,
  COUNT(visitId) AS action_count_visit
  FROM
    action_log_with_status
  )
  
  select Registered_Session,login_status, action_visit, action_count_visit from sub_category_amount
union all select Registered_Session,login_status, action_visit, action_count_visit from category_amount
union all select Registered_Session,login_status, action_visit, action_count_visit from total_amount

order by Registered_Session, login_status desc