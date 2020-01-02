SELECT
  pagepath
  ,count(Distinct Sessions) as access_count
  ,count(Distinct Users) as access_users
  ,sum(Content_View) as Total_CV
  ,sum(Video_View) as Total_VV
FROM `ga360-203507.sample_data_GA.6_1` 
group by 
  pagepath