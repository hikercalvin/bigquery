 SELECT
  TT.date,
  TT.Content_ID,
  ID_date.Content_Title,
  ID_date.Content_Category,
  ID_date.Content_Publish_Date,
  TT.total_Content_View,
  TT.total_Video_View
FROM
  `ga360-203507.sample_data_GA.total_CV_VV` TT
LEFT JOIN
  `ga360-203507.sample_data_GA.Content_ID` ID_date
ON
  TT.Content_ID = ID_date.Content_ID

ORDER BY
    total_Content_View DESC 