SELECT
  date,
  Content_ID,
  Content_Title,
  Content_Category,
  Content_Publish_Date,
  SUM(CASE
      WHEN User_Gender="1" THEN Content_View
  END
    ) AS Count_CV_Male,
  SUM(CASE
      WHEN User_Gender="2" THEN Content_View
  END
    ) AS Count_CV_Female,
  SUM(CASE
      WHEN User_Gender="1" THEN Video_View
  END
    ) AS Count_VV_Male,
  SUM(CASE
      WHEN User_Gender="2" THEN Video_View
  END
    ) AS Count_VV_Female,
  SUM(CASE
      WHEN User_DOB="1" THEN Content_View
  END
    ) AS Count_CV_13,
  SUM(CASE
      WHEN User_DOB="2" THEN Content_View
  END
    ) AS Count_CV_13_17,
  SUM(CASE
      WHEN User_DOB="3" THEN Content_View
  END
    ) AS Count_CV_18_24,
  SUM(CASE
      WHEN User_DOB="4" THEN Content_View
  END
    ) AS Count_CV_25_34,
  SUM(CASE
      WHEN User_DOB="5" THEN Content_View
  END
    ) AS Count_CV_35_44,
  SUM(CASE
      WHEN User_DOB="6" THEN Content_View
  END
    ) AS Count_CV_45_54,
  SUM(CASE
      WHEN User_DOB="7" THEN Content_View
  END
    ) AS Count_CV_55_64,
  SUM(CASE
      WHEN User_DOB="8" THEN Content_View
  END
    ) AS Count_CV_65,
  SUM(CASE
      WHEN User_DOB="1" THEN Video_View
  END
    ) AS Count_VV_13,
  SUM(CASE
      WHEN User_DOB="2" THEN Video_View
  END
    ) AS Count_VV_13_17,
  SUM(CASE
      WHEN User_DOB="3" THEN Video_View
  END
    ) AS Count_VV_18_24,
  SUM(CASE
      WHEN User_DOB="4" THEN Video_View
  END
    ) AS Count_VV_25_34,
  SUM(CASE
      WHEN User_DOB="5" THEN Video_View
  END
    ) AS Count_VV_35_44,
  SUM(CASE
      WHEN User_DOB="6" THEN Video_View
  END
    ) AS Count_VV_45_54,
  SUM(CASE
      WHEN User_DOB="7" THEN Video_View
  END
    ) AS Count_VV_55_64,
  SUM(CASE
      WHEN User_DOB="8" THEN Video_View
  END
    ) AS Count_VV_65
FROM
  `ga360-203507.sample_data_GA.Sessions_log`
GROUP BY
  date,
  Content_ID,
  Content_Title,
  Content_Category,
  Content_Publish_Date
ORDER BY
  Count_CV_Male DESC