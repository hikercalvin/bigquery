WITH
  View_log AS(
  SELECT
    date,
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
      x.index = 1 ) AS Content_ID,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 10 ) AS Content_Title,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 26 ) AS Content_Publish_Date,
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
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 week ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  UNION ALL
  SELECT
    date,
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
      x.index = 1 ) AS Content_ID,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 10 ) AS Content_Title,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 26 ) AS Content_Publish_Date,
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
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 week ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  UNION ALL
  SELECT
    date,
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
      x.index = 1 ) AS Content_ID,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 10 ) AS Content_Title,
    (
    SELECT
      x.value
    FROM
      UNNEST(hit.customDimensions) x
    WHERE
      x.index = 26 ) AS Content_Publish_Date,
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
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 week ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))

 SELECT
    date,
    Content_ID,
    SUM(Content_View) AS total_Content_View,
    SUM(Video_View) AS total_Video_View
  FROM
    View_log
  group by
    date, Content_ID
  order by total_Content_View desc