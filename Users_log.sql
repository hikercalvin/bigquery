with
t_users AS(
  SELECT
    date,
    Users,
    max(OMO_Members) as OMO_ID ,
    max(User_Gender) as Gender_1,
    max(User_DOB) as Age_1,
    count(distinct Sessions) as Total_sessions,
    SUM(Content_View) as Total_content_view,
    SUM(Video_View) as Total_video_view,
    SUM(total_timeOnSite) as Total_time_on_site

from
  `ga360-203507.sample_data_GA.action_log` 
group by date, Users
)

,count_sub as(
  select
    Account__Account_Number,
    max(Rate_Plan_Charge__Created_Date) as Subscribe_time,
    count(Amendment__Type) as Count_sub
FROM
    `nd-data-poc.zuora.ZuoraSubscribe`
group by Account__Account_Number   
)

,count_unsub as(
  select
    Account__Account_Number,
    max(Rate_Plan_Charge__Created_Date) as Unsubscribe_time,
    count(Amendment__Type) as Count_unsub
 FROM
    `nd-data-poc.zuora.ZuoraUnsubscribe`
group by Account__Account_Number  
)

,combine_Subscribers AS(
  SELECT
    ZS.Account__Account_Number,
    Subscribe_time,
    Unsubscribe_time,
    Count_sub,
    Count_unsub,
    case 
    when coalesce(Count_sub,0)-coalesce(Count_unsub,0) > 0 then"升級壹會員"
    when coalesce(Count_sub,0)-coalesce(Count_unsub,0) <= 0 then"取消訂閱壹會員"
    end as User_type_1
  FROM
    count_sub ZS
  left outer JOIN
    count_unsub ZU
  ON
    ZS.Account__Account_Number=ZU.Account__Account_Number)

,combine_users as(
select
  u.date,
  u.Users,
  u.OMO_ID,
  c.Account__Account_Number,
  u.Gender_1,
  case 
    when Gender_1="1" then "Male"
    when Gender_1="2" then "Female"
  end as Gender,
  u.Age_1,
  case
    when Age_1="1" then "<13"
    when Age_1="2" then "13-17"
    when Age_1="3" then "18-24"
    when Age_1="4" then "25-34"
    when Age_1="5" then "35-44"
    when Age_1="6" then "45-54"
    when Age_1="7" then "55-64"
    when Age_1="8" then ">65"
  end as Age,
  u.Total_sessions,
  u.Total_content_view,
  u.Total_video_view,
  u.Total_time_on_site,
  c.Subscribe_time,
  c.Unsubscribe_time,
  c.Count_sub,
  c.Count_unsub,
  c.User_type_1,
  case 
    when User_type_1="升級壹會員" then"升級壹會員"
    when User_type_1="取消訂閱壹會員" then"取消訂閱壹會員"
    when regexp_contains(OMO_ID, '^5') then "已登入OMO會員" else"未登入使用者"   
  end as User_type
from 
  t_users as u 
left outer join
  combine_Subscribers as c
on
  u.OMO_ID = c.Account__Account_Number
order by Subscribe_time desc
)

,combine2_users as(
  select
    date,
    count(distinct Users) as count_users,
    SUM(Total_sessions) as Total_sessions,
    SUM(Total_content_view) as Total_content_view,
    SUM(Total_video_view) as Total_video_view,
    SUM(Total_time_on_site) as Total_time_on_site,
    "升級壹會員" as User_type
  from 
    combine_users
  where 
    User_type="升級壹會員"
  group by date

union all
  
  select
    date,
    count(distinct Users) as count_users,
    SUM(Total_sessions) as Total_sessions,
    SUM(Total_content_view) as Total_content_view,
    SUM(Total_video_view) as Total_video_view,
    SUM(Total_time_on_site) as Total_time_on_site,
    "取消訂閱壹會員" as User_type
  from 
    combine_users
  where 
    User_type="取消訂閱壹會員"
  group by date

union all
  
  select
    date,
    count(distinct Users) as count_users,
    SUM(Total_sessions) as Total_sessions,
    SUM(Total_content_view) as Total_content_view,
    SUM(Total_video_view) as Total_video_view,
    SUM(Total_time_on_site) as Total_time_on_site,
    "已登入OMO會員" as User_type
  from 
    combine_users
  where 
    User_type="已登入OMO會員"
  group by date


union all
  
  select
    date,
    count(distinct Users) as count_users,
    SUM(Total_sessions) as Total_sessions,
    SUM(Total_content_view) as Total_content_view,
    SUM(Total_video_view) as Total_video_view,
    SUM(Total_time_on_site) as Total_time_on_site,
    "未登入使用者" as User_type
  from 
    combine_users
  where 
    User_type="未登入使用者"
  group by date)
  
select
  date,
  count_users,
  Total_sessions,
  Total_content_view,
  Total_video_view,
  Total_time_on_site,
  Total_content_view/count_users as avg_content_view,
  Total_video_view/count_users as avg_video_view,
  Total_time_on_site/count_users as avg_time_on_site,
  User_type
  from
    combine2_users
  order by date desc

