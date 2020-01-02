with 
purchase_log as(
    select
      a.Users,
      a.OMO_Members,
      a.Platform,
      substr(string(timestamp), 1, 10) as dt,
      a.Sessions,
      a.Content_View,
      u.Count_Platform,
      u.User_type
 from
   `ga360-203507.sample_data_GA.action_log` as a 
left outer join
   `ga360-203507.sample_data_GA.mst_users` as u
on a.Users = u.Users 
  )

,max_date as(
select
  max(date(timestamp)) as latest_date
from
   `ga360-203507.sample_data_GA.action_log`
)

,join_max_date as(
  select
    Users,
    OMO_Members,
    Platform,
    dt,
    Sessions,
    Content_View,
    Count_Platform,
    User_type,
    latest_date
  from purchase_log
  cross join max_date 
)

,user_rfm as(
  select
     Users,
     max(User_type) as User_type,
     max(OMO_Members) as OMO_ID,
     max(Count_Platform) as Count_Platform,
     max(dt) as recent_date,
     latest_date,
     date_diff(latest_date, date(timestamp(max(dt))),day) as recency,
     count(distinct dt) as frequency,
     count(distinct Sessions)as Sessions_Count,
     sum(coalesce(Content_View,0))as total_content_view
from
     join_max_date
where User_type="未登入使用者"
group by
    Users,latest_date
)

,avg as(
  select
  sum(recency) as Sum_recency,
  avg(recency) as Avg_recency,
  sum(frequency) as Sum_frequency,
  avg(frequency) as Avg_frequency,
  sum(total_content_view) as Sum_content_view,
  avg(total_content_view) as Avg_content_view 
from
  user_rfm 
)

,median_R as(
  SELECT 
    percentiles[offset(25)] as R_25, 
    percentiles[offset(50)] as R_50,
    percentiles[offset(75)] as R_75
  FROM (SELECT APPROX_QUANTILES(recency, 100) percentiles FROM user_rfm )
)
  
,median_F as(
  SELECT 
    percentiles[offset(25)] as F_25, 
    percentiles[offset(50)] as F_50,
    percentiles[offset(75)] as F_75
  FROM (SELECT APPROX_QUANTILES(frequency , 100) percentiles FROM user_rfm )
)

,median_M as(
  SELECT 
    percentiles[offset(25)] as M_25, 
    percentiles[offset(50)] as M_50,
    percentiles[offset(75)] as M_75
  FROM (SELECT APPROX_QUANTILES(total_content_view , 100) percentiles FROM user_rfm )
)
  
,joinin as(
 select
   u.Users,
   u.OMO_ID,
   u.User_type,
   u.Count_Platform,
   u.recent_date,
   u.latest_date,
   u.recency,
   u.frequency,
   u.Sessions_Count,
   u.total_content_view,
   R_25,
   R_50,
   R_75,
   F_25,
   F_50,
   F_75,
   M_25,
   M_50,
   M_75,
   a.Sum_recency,
   a.Avg_recency,
   a.Sum_frequency,
   a.Avg_frequency,
   a.Sum_content_view,
   a.Avg_content_view
from
   user_rfm as u
cross join
  avg as a
cross join
  median_R 
cross join
  median_F
cross join
  median_M
)


select
  *
from joinin
order by Count_Platform desc