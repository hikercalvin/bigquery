WITH
  View_log AS(
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
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 month ))
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
 )
    

, combine AS (
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

SELECT
  Content_ID,
  Content_Title,
  Content_Category,
  Content_Publish_Date,
  ROW
FROM
  combine
where ROW=1