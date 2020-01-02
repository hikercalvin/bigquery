with 
purchase_log as(
    select
      Users,
      OMO_Members,
      substr(string(timestamp), 1, 10) as dt,
      Sessions,
      Content_View
 from
   `ga360-203507.sample_data_GA.action_log` 
  )

,user_rfm as(
  select
     Users,
     max(OMO_Members) as OMO_ID,
     max(dt) as recent_date,
     date_diff(current_date, date(timestamp(max(dt))),day)-1 as recency,
     count(distinct dt) as frequency,
     count(distinct Sessions)as Sessions_Count,
     sum(coalesce(Content_View,0))as total_content_view
 from
     purchase_log
group by
    Users
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
  
,joinin as(
 select
   u.Users,
   u.OMO_ID,
   u.recent_date,
   u.recency,
   u.frequency,
   u.Sessions_Count,
   u.total_content_view,
   a.Sum_recency,
   a.Avg_recency,
   a.Sum_frequency,
   a.Avg_frequency,
   a.Sum_content_view,
   a.Avg_content_view,
   case 
    when u.recency < a.Avg_recency then "2"
    when u.recency > a.Avg_recency then "1"
   end as r,
   case 
    when u.frequency < a.Avg_frequency then "1"
    when u.frequency > a.Avg_frequency then "2"
   end as f,
    case 
    when u.total_content_view < a.Avg_recency then "1"
    when u.total_content_view > a.Avg_recency then "2"
   end as m
from
   user_rfm as u
cross join
  avg as a
)
,join_user_type as(
  select
    J.Users,
    J.OMO_ID,
    J.recent_date,
    J.recency,
    J.frequency,
    J.Sessions_Count,
    J.total_content_view,
    J.Sum_recency,
    J.Avg_recency,
    J.Sum_frequency,
    J.Avg_frequency,
    J.Sum_content_view,
    J.Avg_content_view,
    U.Gender,
    U.Age,
    U.Subscribe_time,
    U.User_type,
    J.r,
    J.f,
    J.m
  from
    joinin as J
  left join
    `ga360-203507.sample_data_GA.mst_users`  as U
  on J.Users = U.Users
  
)

,concat_rfm as(
  select
  *,
  concat(r,f,m) as rfm
from join_user_type
order by recent_date desc
)

select
  User_type,
  rfm,
  count(distinct Users) as Count_users
from concat_rfm 
group by User_type, rfm
order by User_type, rfm